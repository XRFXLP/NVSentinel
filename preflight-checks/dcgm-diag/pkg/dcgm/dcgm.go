// Package dcgm provides a client for running DCGM diagnostics via dcgmi CLI.
package dcgm

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strings"
)

// DiagLevel represents DCGM diagnostic levels.
type DiagLevel int

const (
	// DiagLevel1 is a quick diagnostic (~30s) - memory, PCIe bandwidth.
	DiagLevel1 DiagLevel = 1
	// DiagLevel2 is an extended diagnostic (~2-3min) - stress, targeted tests.
	DiagLevel2 DiagLevel = 2
	// DiagLevel3 is full diagnostic - comprehensive hardware validation.
	DiagLevel3 DiagLevel = 3
)

// TestResult represents the result of a single DCGM diagnostic test.
type TestResult struct {
	TestName  string // e.g., "Memory", "Diagnostic", "PCIe", "NVLink"
	GPUIndex  int    // GPU index (0, 1, 2, ...), -1 for global tests
	GPUUUID   string // GPU UUID
	Result    string // "Pass", "Fail", "Warn", "Skip"
	Message   string // Additional details/error message
	ErrorCode string // DCGM error code if available
}

// DiagResult contains all results from a DCGM diagnostic run.
type DiagResult struct {
	Level       DiagLevel
	GPUUUIDs    []string
	TestResults []TestResult
	RawOutput   string
	HasFailures bool
	HasWarnings bool
}

// Client runs DCGM diagnostics via the dcgmi CLI.
type Client struct {
	hostengineAddr string
}

// NewClient creates a new DCGM client.
func NewClient(hostengineAddr string) *Client {
	return &Client{hostengineAddr: hostengineAddr}
}

// GetGPUUUIDs retrieves GPU UUIDs using nvidia-smi.
func GetGPUUUIDs(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi failed: %w", err)
	}

	var uuids []string
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		uuid := strings.TrimSpace(scanner.Text())
		if uuid != "" {
			uuids = append(uuids, uuid)
		}
	}

	if len(uuids) == 0 {
		return nil, fmt.Errorf("no GPUs found")
	}

	slog.Info("Discovered GPUs", "count", len(uuids), "uuids", uuids)
	return uuids, nil
}

// RunDiagnostic runs DCGM diagnostics on the specified GPUs.
func (c *Client) RunDiagnostic(ctx context.Context, level DiagLevel, gpuUUIDs []string) (*DiagResult, error) {
	if len(gpuUUIDs) == 0 {
		return nil, fmt.Errorf("no GPU UUIDs provided")
	}

	// Build dcgmi command
	// dcgmi diag -r <level> --host <hostengine> -i <uuid1,uuid2,...>
	args := []string{
		"diag",
		"-r", fmt.Sprintf("%d", level),
	}

	if c.hostengineAddr != "" {
		args = append(args, "--host", c.hostengineAddr)
	}

	args = append(args, "-i", strings.Join(gpuUUIDs, ","))

	slog.Info("Running DCGM diagnostic", "level", level, "gpus", gpuUUIDs, "hostengine", c.hostengineAddr)

	cmd := exec.CommandContext(ctx, "dcgmi", args...)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	// Parse results even if command failed (partial results may be useful)
	result := c.parseOutput(outputStr, level, gpuUUIDs)
	result.RawOutput = outputStr

	if err != nil {
		// Check if it's a diagnostic failure vs command failure
		if result.HasFailures {
			// Diagnostic ran but found issues - this is expected
			slog.Warn("DCGM diagnostic found failures", "level", level)
			return result, nil
		}
		return result, fmt.Errorf("dcgmi command failed: %w, output: %s", err, outputStr)
	}

	return result, nil
}

// parseOutput parses dcgmi diag output into structured results.
func (c *Client) parseOutput(output string, level DiagLevel, gpuUUIDs []string) *DiagResult {
	result := &DiagResult{
		Level:    level,
		GPUUUIDs: gpuUUIDs,
	}

	lines := strings.Split(output, "\n")
	var pendingMessage string

	// Regex patterns for different output formats
	// Pattern 1: "| GPU 0 |" or "GPU 0" (header, not result)
	gpuHeaderPattern := regexp.MustCompile(`^\|\s*GPU\s+(\d+)\s*\|`)

	// Pattern 2: "|   Test Name    | Pass/Fail |" (table format)
	testPattern := regexp.MustCompile(`\|\s*([A-Za-z][A-Za-z0-9\s\-_]*?)\s*\|\s*(Pass|Fail|Warn|Skip|Warning|Error|PASS|FAIL|WARN|SKIP)\s*\|?`)

	// Pattern 3: "Test Name ... Pass" or "Test Name ... Fail" (dotted format)
	dottedPattern := regexp.MustCompile(`^\s*([A-Za-z][A-Za-z0-9\s\-_]*?)\s*\.{2,}\s*(Pass|Fail|Warn|Skip|PASS|FAIL|WARN|SKIP)\s*$`)

	// Pattern 4: Lines with error details
	errorPattern := regexp.MustCompile(`(?i)(error|fail|warning):\s*(.+)`)

	// Pattern 5: GPU-specific results like "| GPU0: Pass |" or "GPU0: Pass"
	gpuResultPattern := regexp.MustCompile(`GPU(\d+):\s*(Pass|Fail|Warn|Skip|PASS|FAIL|WARN|SKIP)`)

	// Track current test name for GPU-specific results
	var currentTestName string

	for i, line := range lines {
		origLine := line
		line = strings.TrimSpace(line)

		// Skip empty lines and separators
		if line == "" || strings.HasPrefix(line, "+--") || strings.HasPrefix(line, "===") {
			continue
		}

		// Check for GPU header row (skip)
		if gpuHeaderPattern.MatchString(line) {
			continue
		}

		// Check for GPU-specific results like "GPU0: Pass" - these belong to the current test
		if matches := gpuResultPattern.FindStringSubmatch(line); len(matches) > 2 {
			gpuIdx := -1
			fmt.Sscanf(matches[1], "%d", &gpuIdx)
			gpuResult := normalizeResult(matches[2])

			gpuUUID := ""
			if gpuIdx >= 0 && gpuIdx < len(gpuUUIDs) {
				gpuUUID = gpuUUIDs[gpuIdx]
			}

			// Use current test name if available
			testName := currentTestName
			if testName == "" {
				testName = "unknown"
			}

			tr := TestResult{
				TestName: testName,
				GPUIndex: gpuIdx,
				GPUUUID:  gpuUUID,
				Result:   gpuResult,
			}

			result.TestResults = append(result.TestResults, tr)

			if gpuResult == "Fail" {
				result.HasFailures = true
			}
			if gpuResult == "Warn" {
				result.HasWarnings = true
			}
			continue
		}

		// Check for test result (table format) - this is an overall test result
		if matches := testPattern.FindStringSubmatch(line); len(matches) > 2 {
			testName := strings.TrimSpace(matches[1])
			testResult := normalizeResult(matches[2])

			// Skip header row
			if strings.EqualFold(testName, "Diagnostic") || strings.EqualFold(testName, "Test") {
				continue
			}

			// Update current test name for subsequent GPU-specific results
			currentTestName = testName

			tr := TestResult{
				TestName: testName,
				GPUIndex: -1, // Overall test result, not GPU-specific
				GPUUUID:  "",
				Result:   testResult,
				Message:  pendingMessage,
			}
			pendingMessage = ""

			result.TestResults = append(result.TestResults, tr)

			if testResult == "Fail" {
				result.HasFailures = true
			}
			if testResult == "Warn" {
				result.HasWarnings = true
			}
			continue
		}

		// Check for dotted format (overall test result)
		if matches := dottedPattern.FindStringSubmatch(line); len(matches) > 2 {
			testName := strings.TrimSpace(matches[1])
			testResult := normalizeResult(matches[2])

			// Update current test name
			currentTestName = testName

			tr := TestResult{
				TestName: testName,
				GPUIndex: -1, // Overall test result
				GPUUUID:  "",
				Result:   testResult,
			}

			result.TestResults = append(result.TestResults, tr)

			if testResult == "Fail" {
				result.HasFailures = true
			}
			if testResult == "Warn" {
				result.HasWarnings = true
			}
			continue
		}

		// Check for error messages to attach to next test
		if matches := errorPattern.FindStringSubmatch(line); len(matches) > 2 {
			pendingMessage = strings.TrimSpace(matches[2])
			// Also look at next line for more context
			if i+1 < len(lines) {
				nextLine := strings.TrimSpace(lines[i+1])
				if !strings.HasPrefix(nextLine, "|") && !strings.HasPrefix(nextLine, "+") && nextLine != "" {
					pendingMessage += " " + nextLine
				}
			}
		}

		// Capture any line that looks like an error message
		if strings.Contains(strings.ToLower(origLine), "fail") ||
			strings.Contains(strings.ToLower(origLine), "error") {
			if pendingMessage == "" {
				pendingMessage = strings.TrimSpace(origLine)
			}
		}
	}

	// If no structured results found but output contains failure indicators
	if len(result.TestResults) == 0 {
		lowerOutput := strings.ToLower(output)
		if strings.Contains(lowerOutput, "fail") || strings.Contains(lowerOutput, "error") {
			result.HasFailures = true
			// Try to extract meaningful error message
			message := "Diagnostic failed. Raw output logged above."
			if pendingMessage != "" {
				message = pendingMessage
			}
			result.TestResults = append(result.TestResults, TestResult{
				TestName: "DiagnosticRun",
				GPUIndex: -1,
				Result:   "Fail",
				Message:  message,
			})
		}
	}

	// Post-processing: If a test has overall "Fail" but all individual GPU results are "Pass",
	// downgrade to "Warn" (this indicates DCGM internal errors, not actual GPU failures)
	downgradeInternalErrorsToWarnings(result)

	return result
}

// downgradeInternalErrorsToWarnings checks if a test failed at the overall level but
// all individual GPUs passed. This typically indicates DCGM infrastructure issues
// (e.g., missing binaries, permission issues) rather than actual GPU failures.
func downgradeInternalErrorsToWarnings(result *DiagResult) {
	// Group results by test name
	testGroups := make(map[string][]TestResult)
	for _, tr := range result.TestResults {
		testGroups[strings.ToLower(tr.TestName)] = append(testGroups[strings.ToLower(tr.TestName)], tr)
	}

	// For each test, check if overall failed but all GPUs passed
	for testName, results := range testGroups {
		var overallResult *TestResult
		var gpuResults []TestResult

		for i := range results {
			if results[i].GPUIndex < 0 {
				overallResult = &results[i]
			} else {
				gpuResults = append(gpuResults, results[i])
			}
		}

		// If overall result is "Fail" but all GPU-specific results are "Pass"
		if overallResult != nil && overallResult.Result == "Fail" && len(gpuResults) > 0 {
			allGPUsPassed := true
			for _, gr := range gpuResults {
				if gr.Result != "Pass" {
					allGPUsPassed = false
					break
				}
			}

			if allGPUsPassed {
				// Downgrade to warning - this is a DCGM internal error, not GPU failure
				for i := range result.TestResults {
					if strings.ToLower(result.TestResults[i].TestName) == testName && result.TestResults[i].GPUIndex < 0 {
						result.TestResults[i].Result = "Warn"
						result.TestResults[i].Message = "(DCGM internal error, all GPUs passed individually) " + result.TestResults[i].Message
					}
				}
			}
		}
	}

	// Recalculate HasFailures and HasWarnings
	result.HasFailures = false
	result.HasWarnings = false
	for _, tr := range result.TestResults {
		if tr.Result == "Fail" {
			result.HasFailures = true
		}
		if tr.Result == "Warn" {
			result.HasWarnings = true
		}
	}
}

// normalizeResult converts various result formats to standard Pass/Fail/Warn/Skip.
func normalizeResult(result string) string {
	switch strings.ToUpper(strings.TrimSpace(result)) {
	case "PASS":
		return "Pass"
	case "FAIL", "ERROR":
		return "Fail"
	case "WARN", "WARNING":
		return "Warn"
	case "SKIP":
		return "Skip"
	default:
		return result
	}
}

// MapTestToRecommendedAction maps DCGM test failures to recommended actions.
func MapTestToRecommendedAction(testName string, result string) (action string, isFatal bool) {
	if result == "Warn" {
		return "NONE", false
	}

	if result != "Fail" {
		return "NONE", false
	}

	// Map based on test name (per design doc)
	testLower := strings.ToLower(testName)
	switch {
	case strings.Contains(testLower, "memory"):
		return "CONTACT_SUPPORT", true
	case strings.Contains(testLower, "pcie"):
		return "CONTACT_SUPPORT", true
	case strings.Contains(testLower, "nvlink"):
		return "CONTACT_SUPPORT", true
	case strings.Contains(testLower, "stress"):
		return "RUN_DCGMEUD", true
	case strings.Contains(testLower, "deployment"):
		return "NONE", false // Software/config issue
	case strings.Contains(testLower, "software"):
		return "NONE", false // Software/config issue
	default:
		return "CONTACT_SUPPORT", true
	}
}
