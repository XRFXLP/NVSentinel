Scheduled a workload to block the drain:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: sticky-workload-10
  namespace: slurm
  finalizers:
    - test.example.com/block-deletion
spec:
  nodeName: "be21d5aa-10"
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.9
```
`
Injected couple of errors for a node

```
  [root@nvidia-device-plugin-daemonset-scgrx /]# chroot /host
# logger -p daemon.err "[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the bus."
# logger -p daemon.err "[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the b"   
# 
```


Verified that there is only one drain CR:

```
$ kg drainrequest -A | grep be21d5aa-10
nvsentinel   drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2   32s

$ kg drainrequest -o yaml drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2 -n nvsentinel
apiVersion: nvsentinel.nvidia.com/v1alpha1
kind: DrainRequest
metadata:
  creationTimestamp: "2026-03-18T02:52:33Z"
  finalizers:
  - nvsentinel.nvidia.com/slinky-drainer
  generation: 1
  labels:
    nvsentinel.nvidia.com/node-name: be21d5aa-10
  name: drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2
  namespace: nvsentinel
  resourceVersion: "90860350"
  uid: dcbf7760-3caa-4ffc-ab59-64d21f83968a
spec:
  checkName: SysLogsXIDError
  entitiesImpacted:
  - type: PCI
    value: "0002:00:00"
  errorCode:
  - "79"
  healthEventID: 69ba13711b2cf7b0574ed8b2
  nodeName: be21d5aa-10
  podsToDrain:
    slurm:
    - sticky-workload-10
    - slurm-worker-cpu-hphqp
  reason: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver],
    GPU has fallen off the bus.'
  recommendedAction: RESTART_BM
```

Both health events are in `InProgress` state.

```
0 [direct: primary] HealthEventsDatabase> db.HealthEvents.find({"healthevent.agent": "syslog-health-monitor"})
[
  {
    _id: ObjectId('69ba13711b2cf7b0574ed8b2'),
    createdAt: ISODate('2026-03-18T02:52:33.672Z'),
    healthevent: {
      version: Long('1'),
      agent: 'syslog-health-monitor',
      componentclass: 'GPU',
      checkname: 'SysLogsXIDError',
      isfatal: true,
      ishealthy: false,
      message: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the bus.',
      recommendedaction: 24,
      errorcode: [ '79' ],
      entitiesimpacted: [ { entitytype: 'PCI', entityvalue: '0002:00:00' } ],
      metadata: {
        'nvidia.com/gpu.product': 'NVIDIA-B200',
        'nvidia.com/cuda.driver-version.major': '575',
        'nvidia.com/cuda.driver-version.minor': '57',
        'nvidia.com/cuda.driver-version.revision': '08',
        'nvidia.com/cuda.driver-version.full': '575.57.08',
        'nvidia.com/cuda.runtime-version.major': '12',
        'nvidia.com/cuda.runtime-version.minor': '9',
        'node.kubernetes.io/instance-type': 'k3s',
        'nvidia.com/cuda.runtime-version.full': '12.9',
        providerID: 'k3s://be21d5aa-10'
      },
      generatedtimestamp: { seconds: Long('1773802353'), nanos: 672251920 },
      nodename: 'be21d5aa-10',
      quarantineoverrides: null,
      drainoverrides: null,
      processingstrategy: 1,
      id: ''
    },
    healtheventstatus: {
      nodequarantined: 'Quarantined',
      userpodsevictionstatus: { status: 'InProgress' },
      faultremediated: null,
      quarantinefinishtimestamp: { seconds: Long('1773802353'), nanos: 739873231 }
    }
  },
  {
    _id: ObjectId('69ba13801b2cf7b0574ed8b3'),
    createdAt: ISODate('2026-03-18T02:52:48.650Z'),
    healthevent: {
      version: Long('1'),
      agent: 'syslog-health-monitor',
      componentclass: 'GPU',
      checkname: 'SysLogsXIDError',
      isfatal: true,
      ishealthy: false,
      message: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the b',
      recommendedaction: 24,
      errorcode: [ '79' ],
      entitiesimpacted: [ { entitytype: 'PCI', entityvalue: '0002:00:00' } ],
      metadata: {
        'nvidia.com/cuda.runtime-version.full': '12.9',
        providerID: 'k3s://be21d5aa-10',
        'node.kubernetes.io/instance-type': 'k3s',
        'nvidia.com/cuda.driver-version.minor': '57',
        'nvidia.com/cuda.driver-version.revision': '08',
        'nvidia.com/cuda.driver-version.major': '575',
        'nvidia.com/cuda.driver-version.full': '575.57.08',
        'nvidia.com/cuda.runtime-version.major': '12',
        'nvidia.com/gpu.product': 'NVIDIA-B200',
        'nvidia.com/cuda.runtime-version.minor': '9'
      },
      generatedtimestamp: { seconds: Long('1773802368'), nanos: 649798012 },
      nodename: 'be21d5aa-10',
      quarantineoverrides: null,
      drainoverrides: null,
      processingstrategy: 1,
      id: ''
    },
    healtheventstatus: {
      nodequarantined: 'AlreadyQuarantined',
      userpodsevictionstatus: { status: 'InProgress' },
      faultremediated: null,
      quarantinefinishtimestamp: { seconds: Long('1773802368'), nanos: 655180865 }
    }
  }
]
```


Patched the workload pod for manual drain:

```
kubectl patch pod sticky-workload-10 -n slurm --type=merge --subresource=status -p '{
  "status": {
    "conditions": [
      {
        "type": "SlurmNodeStateDrain",
        "status": "True"
      }
    ]
  }
}'
```


Drain completed:
```
$ kg drainrequest -n nvsentinel   drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2 -o yaml
apiVersion: nvsentinel.nvidia.com/v1alpha1
kind: DrainRequest
metadata:
  creationTimestamp: "2026-03-18T02:52:33Z"
  deletionGracePeriodSeconds: 0
  deletionTimestamp: "2026-03-18T02:57:03Z"
  finalizers:
  - nvsentinel.nvidia.com/slinky-drainer
  generation: 2
  labels:
    nvsentinel.nvidia.com/node-name: be21d5aa-10
  name: drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2
  namespace: nvsentinel
  resourceVersion: "90863383"
  uid: dcbf7760-3caa-4ffc-ab59-64d21f83968a
spec:
  checkName: SysLogsXIDError
  entitiesImpacted:
  - type: PCI
    value: "0002:00:00"
  errorCode:
  - "79"
  healthEventID: 69ba13711b2cf7b0574ed8b2
  nodeName: be21d5aa-10
  podsToDrain:
    slurm:
    - sticky-workload-10
    - slurm-worker-cpu-hphqp
  reason: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver],
    GPU has fallen off the bus.'
  recommendedAction: RESTART_BM
status:
  conditions:
  - lastTransitionTime: "2026-03-18T02:56:08Z"
    message: All Slinky pods drained successfully
    reason: DrainComplete
    status: "True"
    type: DrainComplete
```


Both events eventually went to drain completion state:
```
rs0 [direct: primary] HealthEventsDatabase> db.HealthEvents.find({"healthevent.agent": "syslog-health-monitor"})
[
  {
    _id: ObjectId('69ba13711b2cf7b0574ed8b2'),
    createdAt: ISODate('2026-03-18T02:52:33.672Z'),
    healthevent: {
      version: Long('1'),
      agent: 'syslog-health-monitor',
      componentclass: 'GPU',
      checkname: 'SysLogsXIDError',
      isfatal: true,
      ishealthy: false,
      message: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the bus.',
      recommendedaction: 24,
      errorcode: [ '79' ],
      entitiesimpacted: [ { entitytype: 'PCI', entityvalue: '0002:00:00' } ],
      metadata: {
        'nvidia.com/gpu.product': 'NVIDIA-B200',
        'nvidia.com/cuda.driver-version.major': '575',
        'nvidia.com/cuda.driver-version.minor': '57',
        'nvidia.com/cuda.driver-version.revision': '08',
        'nvidia.com/cuda.driver-version.full': '575.57.08',
        'nvidia.com/cuda.runtime-version.major': '12',
        'nvidia.com/cuda.runtime-version.minor': '9',
        'node.kubernetes.io/instance-type': 'k3s',
        'nvidia.com/cuda.runtime-version.full': '12.9',
        providerID: 'k3s://be21d5aa-10'
      },
      generatedtimestamp: { seconds: Long('1773802353'), nanos: 672251920 },
      nodename: 'be21d5aa-10',
      quarantineoverrides: null,
      drainoverrides: null,
      processingstrategy: 1,
      id: ''
    },
    healtheventstatus: {
      nodequarantined: 'Quarantined',
      userpodsevictionstatus: { status: 'AlreadyDrained', message: '' },
      faultremediated: null,
      quarantinefinishtimestamp: { seconds: Long('1773802353'), nanos: 739873231 },
      drainfinishtimestamp: { seconds: Long('1773802623'), nanos: 801126952 }
    }
  },
  {
    _id: ObjectId('69ba13801b2cf7b0574ed8b3'),
    createdAt: ISODate('2026-03-18T02:52:48.650Z'),
    healthevent: {
      version: Long('1'),
      agent: 'syslog-health-monitor',
      componentclass: 'GPU',
      checkname: 'SysLogsXIDError',
      isfatal: true,
      ishealthy: false,
      message: 'MESSAGE=[6085126.134786] NVRM: Xid (PCI:0002:00:00): 79, pid=1582259, name=nvc:[driver], GPU has fallen off the b',
      recommendedaction: 24,
      errorcode: [ '79' ],
      entitiesimpacted: [ { entitytype: 'PCI', entityvalue: '0002:00:00' } ],
      metadata: {
        'nvidia.com/cuda.runtime-version.full': '12.9',
        providerID: 'k3s://be21d5aa-10',
        'node.kubernetes.io/instance-type': 'k3s',
        'nvidia.com/cuda.driver-version.minor': '57',
        'nvidia.com/cuda.driver-version.revision': '08',
        'nvidia.com/cuda.driver-version.major': '575',
        'nvidia.com/cuda.driver-version.full': '575.57.08',
        'nvidia.com/cuda.runtime-version.major': '12',
        'nvidia.com/gpu.product': 'NVIDIA-B200',
        'nvidia.com/cuda.runtime-version.minor': '9'
      },
      generatedtimestamp: { seconds: Long('1773802368'), nanos: 649798012 },
      nodename: 'be21d5aa-10',
      quarantineoverrides: null,
      drainoverrides: null,
      processingstrategy: 1,
      id: ''
    },
    healtheventstatus: {
      nodequarantined: 'AlreadyQuarantined',
      userpodsevictionstatus: { status: 'AlreadyDrained', message: '' },
      faultremediated: null,
      quarantinefinishtimestamp: { seconds: Long('1773802368'), nanos: 655180865 },
      drainfinishtimestamp: { seconds: Long('1773802638'), nanos: 695808496 }
    }
  }
]
```

Verified after a while that we still have just one CR:
```
$ kg drainrequest -A | grep drain-be21d5aa-10
nvsentinel   drain-be21d5aa-10-69ba13711b2cf7b0574ed8b2   56m
```

