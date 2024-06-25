package supervisedtree

type SupervisedTree interface {
	Train() error
	Predict(input []float64) ([]float64, error)
	Evaluate() (float64, error)
}
