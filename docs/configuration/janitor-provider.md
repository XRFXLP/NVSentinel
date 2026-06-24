# Janitor Provider Configuration

## Overview

The janitor-provider module executes node lifecycle operations requested by Janitor, including reboot signals, node readiness checks, and provider-specific termination signals. This document covers the Helm configuration options for selecting and configuring the provider backend.

## Configuration Reference

### Module Enable/Disable

Controls whether the janitor-provider module is deployed in the cluster.

```yaml
global:
  janitorProvider:
    enabled: true
```

### CSP Provider

Selects the provider implementation used by janitor-provider.

```yaml
janitor-provider:
  csp:
    provider: "kind"
```

Supported providers are `kind`, `kwok`, `aws`, `gcp`, `azure`, `oci`, `nebius`, and `generic`.

## Generic Provider

The `generic` provider is intended for bare-metal and on-premises clusters where there is no cloud provider reboot API. It creates a privileged Kubernetes Job on the target node and uses the node `bootID` to verify that a reboot occurred.

```yaml
janitor-provider:
  csp:
    provider: "generic"
    generic:
      rebootImage: "public.ecr.aws/docker/library/busybox:1.37.0"
      useSysrqReboot: false
      rebootJobNamespace: ""
      rebootJobTTLSeconds: 3600
      imagePullSecrets: ""
```

### Generic Provider Options

`rebootImage` sets the container image used for the privileged reboot Job.

`useSysrqReboot` switches the reboot command from `chroot /host reboot` to Linux Magic SysRq by writing `b` to the host `/proc/sysrq-trigger`. Keep this disabled unless the standard reboot path leaves nodes stuck `NotReady`.

`rebootJobNamespace` sets the namespace where reboot Jobs are created. If empty, the janitor-provider release namespace is used.

`rebootJobTTLSeconds` controls how long completed reboot Jobs are retained before Kubernetes garbage-collects them.

`imagePullSecrets` is a comma-separated list of image pull secret names used by the reboot Job.

### SysRq Reboot

When `useSysrqReboot` is enabled, janitor-provider sets `GENERIC_REBOOT_USE_SYSRQ=true` and the reboot Job writes to the host SysRq trigger instead of invoking the normal reboot command.

This path bypasses the normal userspace shutdown flow. It is useful for environments where `chroot /host reboot` or `sudo reboot` is accepted but leaves the node stuck `NotReady`, but it should remain an explicit opt-in because it is more abrupt than the default reboot path.
