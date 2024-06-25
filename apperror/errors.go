package apperror

type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3
)

type APPError struct {
	Type   ErrorType
	Reason string
}

func (e APPError) Error() string {
	return e.Reason
}

func (e APPError) GetType() ErrorType {
	return e.Type
}
