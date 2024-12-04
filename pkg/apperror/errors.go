package apperror

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	tierrors "github.com/pingcap/tiflow/pkg/errors"
)

var (
	ErrChangefeedRetryable = errors.Normalize(
		"changefeed is in retryable state",
		errors.RFCCodeText("CDC:ErrChangefeedRetryable"),
	)
	ErrChangefeedInitTableTriggerEventDispatcherFailed = errors.Normalize(
		"failed to init table trigger event dispatcher",
		errors.RFCCodeText("CDC:ErrChangefeedInitTableTriggerEventDispatcherFailed"),
	)
)

type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3

	ErrorTypeInvalid    ErrorType = 101
	ErrorTypeIncomplete ErrorType = 102
	ErrorTypeDecodeData ErrorType = 103
	ErrorTypeBufferFull ErrorType = 104
	ErrorTypeDuplicate  ErrorType = 105
	ErrorTypeNotExist   ErrorType = 106
	ErrorTypeClosed     ErrorType = 107

	ErrorTypeConnectionFailed     ErrorType = 201
	ErrorTypeConnectionNotFound   ErrorType = 202
	ErrorTypeMessageCongested     ErrorType = 204
	ErrorTypeMessageReceiveFailed ErrorType = 205
	ErrorTypeMessageSendFailed    ErrorType = 206
	ErrorTypeTargetNotFound       ErrorType = 207
	ErrorTypeInvalidMessage       ErrorType = 208

	//ErrorTypeCreateEventDispatcherManagerFailed ErrorType = 300

	ErrorInvalidDDLEvent ErrorType = 301
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
	case ErrorTypeDecodeData:
		return "DecodeData"
	case ErrorTypeBufferFull:
		return "BufferFull"
	case ErrorTypeDuplicate:
		return "Duplicate"
	case ErrorTypeNotExist:
		return "NotExist"
	case ErrorTypeClosed:
		return "Closed"
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

func NewAppErrorS(t ErrorType) *AppError {
	return &AppError{
		Type:   t,
		Reason: "",
	}
}

func NewAppError(t ErrorType, reason string) *AppError {
	return &AppError{
		Type:   t,
		Reason: reason,
	}
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

var changefeedUnRetryableErrors = []*errors.Error{
	tierrors.ErrExpressionColumnNotFound,
	tierrors.ErrExpressionParseFailed,
	tierrors.ErrSchemaSnapshotNotFound,
	tierrors.ErrSyncRenameTableFailed,
	tierrors.ErrChangefeedUnretryable,
	tierrors.ErrCorruptedDataMutation,
	tierrors.ErrDispatcherFailed,
	tierrors.ErrColumnSelectorFailed,

	tierrors.ErrSinkURIInvalid,
	tierrors.ErrKafkaInvalidConfig,
	tierrors.ErrMySQLInvalidConfig,
	tierrors.ErrStorageSinkInvalidConfig,

	// gc related errors
	tierrors.ErrGCTTLExceeded,
	tierrors.ErrSnapshotLostByGC,
	tierrors.ErrStartTsBeforeGC,
}

// ErrorCode returns the RFC error code for the given error.
// If the error is a changefeed unretryable error, returns ErrChangefeedUnretryable.
// otherwise, return ErrChangefeedRetryable
func ErrorCode(err error) errors.RFCErrorCode {
	for _, e := range changefeedUnRetryableErrors {
		if e.Equal(err) {
			return tierrors.ErrChangefeedUnretryable.RFCCode()
		}
		if code, ok := tierrors.RFCCode(err); ok {
			if code == e.RFCCode() {
				return tierrors.ErrChangefeedUnretryable.RFCCode()
			}
		}
		if strings.Contains(err.Error(), string(e.RFCCode())) {
			return tierrors.ErrChangefeedUnretryable.RFCCode()
		}
	}

	return ErrChangefeedRetryable.RFCCode()
}
