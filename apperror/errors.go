package apperror

type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3

	ErrorTypeInvalid    ErrorType = 101
	ErrorTypeImcomplete ErrorType = 102

	ErrorTypeConnectionFailed   ErrorType = 201
	ErrorTypeConnectionNotFound ErrorType = 202
	ErrorTypeSendMessageFailed  ErrorType = 203
)

type AppError struct {
	Type   ErrorType
	Reason string
}

func (e AppError) Error() string {
	return e.Reason
}

func (e AppError) GetType() ErrorType {
	return e.Type
}
