package rpc

import (
	"encoding/json"
)

const AddLogsPayloadType = "addlogsmsg"

// LogEntry is one buffered log line in a batched AddLogs request. Only the line
// content and its client-side timestamp are carried; the colony and executor are
// derived on the server from the authenticated caller.
type LogEntry struct {
	Timestamp int64  `json:"timestamp"` // client/event time (UTC Unix nanos)
	Message   string `json:"message"`
}

// AddLogsMsg carries many log lines for a single process in one signed request.
type AddLogsMsg struct {
	ProcessID string     `json:"processid"`
	Entries   []LogEntry `json:"entries"`
	MsgType   string     `json:"msgtype"`
}

func CreateAddLogsMsg(processID string, entries []LogEntry) *AddLogsMsg {
	msg := &AddLogsMsg{}
	msg.ProcessID = processID
	msg.Entries = entries
	msg.MsgType = AddLogsPayloadType

	return msg
}

func (msg *AddLogsMsg) ToJSON() (string, error) {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (msg *AddLogsMsg) ToJSONIndent() (string, error) {
	jsonBytes, err := json.MarshalIndent(msg, "", "    ")
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (msg *AddLogsMsg) Equals(msg2 *AddLogsMsg) bool {
	if msg2 == nil {
		return false
	}

	if msg.MsgType != msg2.MsgType || msg.ProcessID != msg2.ProcessID {
		return false
	}
	if len(msg.Entries) != len(msg2.Entries) {
		return false
	}
	for i := range msg.Entries {
		if msg.Entries[i] != msg2.Entries[i] {
			return false
		}
	}

	return true
}

func CreateAddLogsMsgFromJSON(jsonString string) (*AddLogsMsg, error) {
	var msg *AddLogsMsg

	err := json.Unmarshal([]byte(jsonString), &msg)
	if err != nil {
		return msg, err
	}

	return msg, nil
}
