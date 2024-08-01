package threadpool

// The benchmark result of https://gist.github.com/timstclair/be93968572d36e03cc4c :
// goos: darwin
// goarch: amd64
// cpu: Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz
// BenchmarkReflectSelect-6                            	       2	 969838600 ns/op	723502464 B/op	10245323 allocs/op
// BenchmarkGoSelect-6                                 	      25	  45379138 ns/op	   20451 B/op	     419 allocs/op
// BenchmarkSanity-6                                   	   22381	     52417 ns/op	       0 B/op	       0 allocs/op
//
// Conclusion: reflect.SelectCase is way more expensive than using multiple goroutines and channels.
// Done't use reflect.SelectCase!
