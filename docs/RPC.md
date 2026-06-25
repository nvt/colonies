# HTTP RPC protocol
The Colonies RPC messages has the following format:

```json
{
    "payloadtype": "addcolonymsg",
    "payload": "ewogICAgICBjb2xvbnlpZDogYWM4ZGM4OTQ5YWYzOTVmZDUxZWFkMzFkNTk4YjI1MmJkYTAyZjFmNmVlZDExYWNlN2ZjN2RjOGRkODVhYzMyZSwKICAgICAgbmFtZTogdGVzdF9jb2xvbnlfbmFtZQogIH0=",
    "signature": "82f2ba6368d5c7d0e9bfa6a01a8fa4d4263113f9eedf235e3a4c7b1febcdc2914fe1f8727746b2f501ceec5736457f218fe3b1a469dd6071775c472a802aa81501",
}
```

* Messages are POSTed to http://host:port/api.
* The *payload* attribute is an Base64 string containing JSON data as specified in the API description below.
* The *signature* is calculated based on the Base64 payload data using a private key.
* It is assumed that SSL/TLS are used to prevent replay attacks.
* Note that **payloadtype** and **msgtype** must match. The reason to duplicate this information is allow for introspection using structured parsning but at the same time sign the message so that the semantic of the RPC operation is kept in one message. Otherwise, an attacker would be able to change the payloadtype and keep the payload to trick the Colonies Server. 

The Colonies Server will reply with a RPC reply message according to the following format:

```json
{
    "payloadtype": "addcolonymsg",
    "payload": "ewogICAgICBjb2xvbnlpZDogNmQ2MWFmZTc5MTRjNjNmMjhhNGM5NzY0NWNlNmFiMjY0YzNhZDNhMGU0NmViZDFmMzc4OGU4MzA1MzkzNGUxOCwKICAgICAgbmFtZTogdGVzdF9jb2xvbnlfbmFtZQogIH0=",
}
```

If the **payloadtype** is set to **error**, then the payload will contain the following JSON data:
```json
{
    "status": "500",
    "message": "something when wrong here"
}
```

Else it will contain the reply JSON data, e.g:
```json
{
    "colonyid": "6d61afe7914c63f28a4c97645ce6ab264c3ad3a0e46ebd1f3788e83053934e18",
    "name": "test_colony_name"
}
```

## Colony API

### Add Colony
* PayloadType: **addcolonymsg**
* Credentials: A valid Server Owner Private Key

#### Payload 
```json
{
    "msgtype": "addcolonymsg",
    "colony": {
        "colonyid": "6d61afe7914c63f28a4c97645ce6ab264c3ad3a0e46ebd1f3788e83053934e18",
        "name": "test_colony_name"
    }
}
```

#### Reply 
```json
{
    "colonyid": "6d61afe7914c63f28a4c97645ce6ab264c3ad3a0e46ebd1f3788e83053934e18",
    "name": "test_colony_name"
}
```

### Remove Colony
* PayloadType: **removecolonymsg**
* Credentials: A valid Server Owner Private Key

#### Payload 
```json
{
    "msgtype": "removecolonymsg",
    "colonyname": "test_colony_name"
}
```

#### Reply 
```json
{}
```

### List Colonies
* PayloadType: **getcoloniesmsg**
* Credentials: A valid Server Owner Private Key

#### Payload 
```json
{
    "payloadtype": "getcoloniesmsg",
    "payload": "...",
    "signature": "...",
}
```

#### Reply 
```json
[
    {
        "colonyid": "aaae394349008b01c4e56c57a5069aa2e2e8c7e41d9118e04a9039b90b41e93c",
        "name": "test_colony_name"
    },
    {
        "colonyid": "f3127b8c82942e023a8d0b9964203fa00dc22bf7b120e26059d640edeabeb11d",
        "name": "test_colony_name_2"
    }
]
```

### Get Colony info
* PayloadType: **getcolonymsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "getcolonymsg",
    "colonyname": "test_colony_name"
}
```

#### Reply 
```json
{
    "colonyid": "ac8dc8949af395fd51ead31d598b252bda02f1f6eed11ace7fc7dc8dd85ac32e",
    "name": "test_colony_name"
}
```

## Executor API

An Executor is described by the following JSON structure. The hardware and
software capabilities are reported under the nested **capabilities** object.

```json
{
    "executorid": "38df5bbbcf0ccb438d2e4151638e3967bf28a5654af6a7e5acc590c0e49fae06",
    "executortype": "test_executor_type",
    "executorname": "test_executor_name",
    "colonyname": "test_colony_name",
    "state": 0,
    "requirefuncreg": false,
    "commissiontime": "2022-01-02T11:58:30.017857Z",
    "lastheardfromtime": "2022-01-02T11:58:30.017857Z",
    "capabilities": {
        "hardware": [
            {
                "model": "AMD Ryzen 9 5950X",
                "nodes": 1,
                "cpu": "AMD Ryzen 9 5950X (32) @ 3.400GHz",
                "cores": 32,
                "mem": "80326Mi",
                "storage": "1Ti",
                "gpu": {
                    "name": "NVIDIA GeForce RTX 2080 Ti Rev. A",
                    "mem": "11Gi",
                    "count": 1,
                    "nodecount": 1
                },
                "platform": "linux",
                "architecture": "amd64",
                "network": ["192.168.1.100"]
            }
        ],
        "software": []
    },
    "allocations": {
        "projects": {}
    }
}
```

The **state** field can have the following values:
* 0 : Pending
* 1 : Approved
* 2 : Rejected
* 3 : Unregistered

### Add Executor
* PayloadType: **addexecutormsg**
* Credentials: A valid Colony Private Key

#### Payload 
```json
{
    "msgtype": "addexecutormsg",
    "executor": {
        "executorid": "38df5bbbcf0ccb438d2e4151638e3967bf28a5654af6a7e5acc590c0e49fae06",
        "executortype": "test_executor_type",
        "executorname": "test_executor_name",
        "colonyname": "test_colony_name",
        "state": 0
    }
}
```

#### Reply 
```json
{
    "executorid": "38df5bbbcf0ccb438d2e4151638e3967bf28a5654af6a7e5acc590c0e49fae06",
    "executortype": "test_executor_type",
    "executorname": "test_executor_name",
    "colonyname": "test_colony_name",
    "state": 0
}
```

### List Executors
* PayloadType: **getexecutorsmsg**
* Credentials: A valid Executor or Colony Private Key

#### Payload 
```json
{
    "msgtype": "getexecutorsmsg",
    "colonyname": "test_colony_name"
}
```

#### Reply 
```json
[
    {
        "executorid": "9525365b67efdbbf37bc1fa7628c7e75bafd2f298cd26f75500bc1364b2c4c1c",
        "executortype": "test_executor_type",
        "executorname": "test_executor_name",
        "colonyname": "test_colony_name",
        "state": 1
    }
]
```

### Get Executor info
* PayloadType: **getexecutormsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "getexecutormsg",
    "colonyname": "test_colony_name",
    "executorname": "test_executor_name"
}
```

#### Reply 
```json
{
    "executorid": "ed2aa78eabe3d1f6fd46ef1247199e9a12faf1a8f1bcba0db51265515c3f08e0",
    "executortype": "test_executor_type",
    "executorname": "test_executor_name",
    "colonyname": "test_colony_name",
    "state": 2
}
```

### Approve Executor 
* PayloadType: **approveexecutormsg**
* Credentials: A valid Colony Private Key

#### Payload
```json
{
    "msgtype": "approveexecutormsg",
    "colonyname": "test_colony_name",
    "executorname": "test_executor_name"
}
```

#### Reply
```json
{}
```

### Reject Executor 
* PayloadType: **rejectexecutormsg**
* Credentials: A valid Colony Private Key

#### Payload 
```json
{
    "msgtype": "rejectexecutormsg",
    "colonyname": "test_colony_name",
    "executorname": "test_executor_name"
}
```

#### Reply 
```json
{}
```

### Remove Executor
* PayloadType: **removeexecutormsg**
* Credentials: A valid Colony Private Key

#### Payload 
```json
{
    "msgtype": "removeexecutormsg",
    "colonyname": "test_colony_name",
    "executorname": "test_executor_name"
}
```

#### Reply 
```json
{}
```

## Process API

A FunctionSpec describes the work to run. Its JSON structure is:

```json
{
    "nodename": "test_node",
    "funcname": "test_func",
    "args": ["arg1"],
    "kwargs": {"key": "value"},
    "priority": 0,
    "maxwaittime": -1,
    "maxexectime": -1,
    "maxretries": 3,
    "conditions": {
        "colonyname": "test_colony_name",
        "executornames": [],
        "executortype": "test_executor_type",
        "dependencies": [],
        "nodes": 1,
        "processes": 1,
        "processespernode": 1,
        "cpu": "1000m",
        "mem": "1000Mi",
        "storage": "10Gi",
        "gpu": {
            "name": "",
            "mem": "0",
            "count": 0,
            "nodecount": 0
        },
        "walltime": 0
    },
    "label": "test_label"
}
```

A Process wraps a FunctionSpec together with its scheduling state:

```json
{
    "processid": "2c0fd0407292538cb8dce3cb306f88b2ab7f3726d649e07502eb04344d9f7164",
    "initiatorid": "",
    "initiatorname": "",
    "assignedexecutorid": "",
    "isassigned": false,
    "state": 0,
    "submissiontime": "2022-01-02T11:58:30.017857Z",
    "starttime": "0001-01-01T00:00:00Z",
    "endtime": "0001-01-01T00:00:00Z",
    "waitdeadline": "0001-01-01T00:00:00Z",
    "execdeadline": "0001-01-01T00:00:00Z",
    "retries": 0,
    "attributes": [],
    "spec": { },
    "waitforparents": false,
    "parents": [],
    "children": [],
    "processgraphid": "",
    "in": null,
    "out": null,
    "errors": []
}
```

### Submit Function Specification
* PayloadType: **submitfuncspecmsg**
* Credentials: A valid Executor Private Key

#### Payload
```json
{
    "msgtype": "submitfuncspecmsg",
    "spec": {
        "funcname": "test_func",
        "args": ["arg1"],
        "maxwaittime": -1,
        "maxexectime": -1,
        "maxretries": 3,
        "conditions": {
            "colonyname": "test_colony_name",
            "executornames": [],
            "executortype": "test_executor_type"
        }
    }
}
```

#### Reply
The reply is a Process object (see above) describing the submitted process.

### Assign Process to an Executor
* PayloadType: **assignprocessmsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "assignprocessmsg",
    "colonyname": "test_colony_name",
    "timeout": 10,
    "availablecpu": "",
    "availablemem": ""
}
```

#### Reply 
The reply is a Process object (see above) with **state** set to 1 (Running)
and **assignedexecutorid** set to the assigned executor.

### List process history
* PayloadType: **getprocesshistmsg**
* Credentials: A valid Executor or Colony Private Key

#### Payload 
The state attribute can have the following values:
* 0 : Waiting 
* 1 : Running 
* 2 : Success 
* 3 : Failed 

Note, all processes will be returned for the entire colony if executorid is not specified.

```json
{
    "msgtype": "getprocesshistmsg",
    "colonyname": "test_colony_name",
    "executorid": "",
    "seconds": 100,
    "state": 3 
}
```

#### Reply 
An array of Process objects (see above).

### List processes
* PayloadType: **getprocessesmsg**
* Credentials: A valid Executor or Colony Private Key

#### Payload 
The state attribute can have the following values:
* 0 : Waiting 
* 1 : Running 
* 2 : Success 
* 3 : Failed 

```json
{
    "msgtype": "getprocessesmsg",
    "colonyname": "test_colony_name",
    "count": 2,
    "state": 3 
}
```

#### Reply 
An array of Process objects (see above).

### Get Process info
* PayloadType: **getprocessmsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "getprocessmsg",
    "processid": "80a98f46c7a364fd33339a6fb2e6c5d8988384fdbf237b4012490c4658bbc9ce"
}
```

#### Reply 
A Process object (see above).

### Remove Process
* PayloadType: **removeprocessmsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "removeprocessmsg",
    "processid": "80a98f46c7a364fd33339a6fb2e6c5d8988384fdbf237b4012490c4658bbc9ce"
}
```

#### Reply 
```json
{}
```

### Remove all Processes
* PayloadType: **removeallprocessesmsg**
* Credentials: A valid Colony Private Key

#### Payload 
```json
{
    "msgtype": "removeallprocessesmsg",
    "colonyname": "test_colony_name",
    "state": 3
}
```

#### Reply 
```json
{}
```

### Close Process as Successful 
* PayloadType: **closesuccessfulmsg**
* Credentials: A valid Executor Private Key and the Executor ID needs to match the ExecutorID assigned to the process

#### Payload 
```json
{
    "msgtype": "closesuccessfulmsg",
    "processid": "ed041355071d2ee6d0ec27b480e2e4c8006cf465ec408b57fcdaa5dac76af8e2"
}
```

#### Reply
```json
{}
```

### Close a Process as Failed
* PayloadType: **closefailedmsg**
* Credentials: A valid Executor Private Key and the Executor ID needs to match the ExecutorID assigned to the process

#### Payload 
```json
{
    "msgtype": "closefailedmsg",
    "processid": "24f6d85804e2abde0c85a9e8aef8b308c44a72323565b14f11756d4997acf200"
}
```

#### Reply 
```json
{}
```

### Colony Statistics
* PayloadType: **getcolonystatsmsg**
* Credentials: A valid Executor or Colony Private Key

#### Payload 
```json
{
    "msgtype": "getcolonystatsmsg",
    "colonyname": "test_colony_name"
}
```

#### Reply 
```json
{
    "colonies": 1,
    "executors": 2,
    "activeexecutors": 2,
    "unregisteredexecutors": 0,
    "waitingprocesses": 1,
    "runningprocesses": 2,
    "successfulprocesses": 3,
    "failedprocesses": 4,
    "waitingworkflows": 0,
    "runningworkflows": 0,
    "successfulworkflows": 0,
    "failedworkflows": 0
}
```


### Add Attribute to a Process 
* PayloadType: **addattributemsg**
* Credentials: A valid Executor Private Key and the Executor ID needs to match the ExecutorID assigned to the process

#### Payload 
```json
{
    "msgtype": "addattributemsg",
    "attribute": {
        "attributeid": "216e26cb089032d2f941454e7db5f3ae1591eeb43eb477c3f8ed545b96d4f690",
        "targetid": "c4775cab695da8a77b503bbe29df8ae39dafd1c7fed3275dac11b436c1724dbf",
        "attributetype": 1,
        "key": "result",
        "value": "helloworld"
    }
}
```

#### Reply 
```json
{
    "attributeid": "216e26cb089032d2f941454e7db5f3ae1591eeb43eb477c3f8ed545b96d4f690",
    "targetid": "c4775cab695da8a77b503bbe29df8ae39dafd1c7fed3275dac11b436c1724dbf",
    "attributetype": 1,
    "key": "result",
    "value": "helloworld"
}
```

### Get Attribute assigned to a Process 
* PayloadType: **getattributemsg**
* Credentials: A valid Executor Private Key

#### Payload 
```json
{
    "msgtype": "getattributemsg",
    "attributeid": "a1d8f3613e074a250c2fbab478a0e11eb40defee66bd9b6a6ceb96990f1486eb"
}
```

#### Reply 
```json
{
    "attributeid": "a1d8f3613e074a250c2fbab478a0e11eb40defee66bd9b6a6ceb96990f1486eb",
    "targetid": "3d893a44a30c7e5c5c595413a9de1545a9d43a844528831c4e205b280c074e56",
    "attributetype": 1,
    "key": "result",
    "value": "helloworld"
}
```

### Subscribe Process Events
* PayloadType: **subscribeprocessmsg**
* Credentials: A valid Executor Private Key
* Comments: Receives an event when a process changes state. The payload needs to be sent over a websocket to: wss://host:port/pubsub

#### Payload 
The state attribute can have the following values:
* 0 : Waiting
* 1 : Running
* 2 : Success
* 3 : Failed

```json
{
    "msgtype": "subscribeprocessmsg",
    "colonyname": "test_colony_name",
    "processid": "80a98f46c7a364fd33339a6fb2e6c5d8988384fdbf237b4012490c4658bbc9ce",
    "executortype": "test_executor_type",
    "state": 1,
    "timeout": -1
}
```

#### Reply 
A Process object (see above) is delivered over the websocket whenever the
process reaches the requested state.

### Subscribe Processes Events
* PayloadType: **subscribeprocessesmsg**
* Credentials: A valid Executor Private Key
* Comments: Receives an event when processes are added or change state. The payload needs to be sent over a websocket to: wss://host:port/pubsub

#### Payload 
The state attribute can have the following values:
* 0 : Waiting
* 1 : Running
* 2 : Success
* 3 : Failed

```json
{
    "msgtype": "subscribeprocessesmsg",
    "colonyname": "test_colony_name",
    "executortype": "test_executor_type",
    "state": 1,
    "timeout": -1
}
```

#### Reply 
A Process object (see above) is delivered over the websocket whenever a matching
process is added or reaches the requested state.
</content>
</invoke>
