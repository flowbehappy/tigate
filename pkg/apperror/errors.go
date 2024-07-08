package apperror

import "fmt"

type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3

	ErrorTypeInvalid    ErrorType = 101
	ErrorTypeIncomplete ErrorType = 102

	ErrorTypeConnectionFailed     ErrorType = 201
	ErrorTypeConnectionNotFound   ErrorType = 202
	ErrorTypeMessageCongested     ErrorType = 204
	ErrorTypeMessageReceiveFailed ErrorType = 205
	ErrorTypeMessageSendFailed    ErrorType = 206
	ErrorTypeTargetNotFound       ErrorType = 207
)

func (t ErrorType) String() string {
	switch t {
	case ErrorTypeUnknown:
		return "Unknown"
	case ErrorTypeEpochMismatch:
		return "EpochMismatch"
	case ErrorTypeEpochSmaller:
		return "EpochSmaller"
	case ErrorTypeTaskIDMismatch:
		return "TaskIDMismatch"
	case ErrorTypeInvalid:
		return "Invalid"
	case ErrorTypeIncomplete:
		return "Incomplete"
	case ErrorTypeConnectionFailed:
		return "ConnectionFailed"
	case ErrorTypeConnectionNotFound:
		return "ConnectionNotFound"
	case ErrorTypeMessageCongested:
		return "MessageCongested"
	case ErrorTypeMessageReceiveFailed:
		return "MessageReceiveFailed"
	case ErrorTypeMessageSendFailed:
		return "MessageSendFailed"
	default:
		return "Unknown"
	}
}

type AppError struct {
	Type   ErrorType
	Reason string
}

func (e AppError) Error() string {
	return fmt.Sprintf("ErrorType: %s, Reason: %s", e.Type, e.Reason)
}

func (e AppError) GetType() ErrorType {
	return e.Type
}

func (e AppError) Equal(err AppError) bool {
	return e.Type == err.Type
}
