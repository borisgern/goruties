package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, j := range jobs {
		out := make(chan interface{})
		wg.Add(1)
		go func(j job, in, out chan interface{}) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(j, in, out)
		in = out
	}
	wg.Wait()
}

// func SingleHash(in, out chan interface{}) {
// 	allLeftCrc32 := make(map[int]string)
// 	wg := &sync.WaitGroup{}
// 	mu := &sync.Mutex{}

// 	for v := range in {
// 		wg.Add(1)
// 		go func(v int, allLeftCrc32 map[int]string, mu *sync.Mutex) {
// 			defer wg.Done()
// 			data := DataSignerCrc32(strconv.Itoa(v))
// 			fmt.Printf("add %v to allLeftCrc32[%v]\n", data, v)
// 			mu.Lock()
// 			allLeftCrc32[v] = data
// 			mu.Unlock()
// 		}(v.(int), allLeftCrc32, mu)

// 		// go func() {
// 		// 	wg.Wait()
// 		// 	fmt.Printf("finished allLeftCrc32 %v\n", allLeftCrc32)
// 		// }()
// 		rightMD5 := DataSignerMd5(strconv.Itoa(v.(int)))
// 		rightCrc32 := DataSignerCrc32(rightMD5)
// 		wg.Wait()
// 		mu.Lock()
// 		fmt.Printf("single res %v\n", allLeftCrc32[v.(int)]+"~"+rightCrc32)
// 		out <- allLeftCrc32[v.(int)] + "~" + rightCrc32
// 		mu.Unlock()
// 	}
// }

// func MultiHash(in, out chan interface{}) {
// 	wg := &sync.WaitGroup{}
// 	mu := &sync.Mutex{}
// 	allCrc32 := make(map[int]string)
// 	for v := range in {
// 		var res string
// 		wg.Add(1)
// 		go func(v string, allCrc32 map[int]string, mu *sync.Mutex) {
// 			defer wg.Done()
// 			for i := 0; i < 6; i++ {
// 				data := DataSignerCrc32(strconv.Itoa(i) + v)
// 				fmt.Printf("add %v to allCrc32[%v]\n", data, i)
// 				mu.Lock()
// 				allCrc32[i] = data
// 				mu.Unlock()
// 			}
// 		}(v.(string), allCrc32, mu)
// 		wg.Wait()
// 		for i := 0; i < len(allCrc32); i++ {
// 			mu.Lock()
// 			res += allCrc32[i]
// 			mu.Unlock()
// 		}
// 		fmt.Printf("multi res %v\n", res)
// 		out <- res
// 	}
// }

func SingleHash(in, out chan interface{}) {
	for v := range in {
		fmt.Printf("in single, data %v\n", v)
		left := DataSignerCrc32(strconv.Itoa(v.(int)))
		rightMD5 := DataSignerMd5(strconv.Itoa(v.(int)))
		right := DataSignerCrc32(rightMD5)
		fmt.Printf("single res %v\n", left+"~"+right)
		out <- left + "~" + right
	}
}

// func SingleHash(in, out chan interface{}) {
// 	fmt.Printf("in single\n")
// 	type single struct {
// 		r string
// 		l string
// 	}
// 	type half struct {
// 		halfType int // 1 - right, 0 - left
// 		parent   int
// 		value    string
// 	}
// 	res := make(chan half)

// 	for v := range in {
// 		fmt.Printf("in single, data %v\n", v)
// 		go func(v int) {
// 			res <- half{halfType: 1, parent: v, value: DataSignerCrc32(strconv.Itoa(v))}
// 		}(v.(int))
// 		go func(v int) {
// 			rightMD5 := DataSignerMd5(strconv.Itoa(v))
// 			res <- half{halfType: 1, parent: v, value: DataSignerCrc32(rightMD5)}
// 		}(v.(int))
// 		go func() {
// 			check := make(map[int]single)
// 			for halfHash := range res {
// 				if val, ok := check[halfHash.parent]; !ok {
// 					if halfHash.halfType == 1 {
// 						check[halfHash.parent] = single{r: halfHash.value}
// 					} else {
// 						check[halfHash.parent] = single{l: halfHash.value}
// 					}
// 				} else {
// 					if halfHash.halfType == 1 {
// 						fmt.Printf("single res %v\n", halfHash.value+"~"+val.r)
// 						out <- halfHash.value + "~" + val.r
// 					} else {
// 						fmt.Printf("single res %v\n", val.l+"~"+halfHash.value)
// 						out <- val.l + "~" + halfHash.value
// 					}
// 				}
// 			}
// 		}()
// 	}
// }

func MultiHash(in, out chan interface{}) {

	for v := range in {
		var res string
		for i := 0; i < 6; i++ {
			res += DataSignerCrc32(strconv.Itoa(i) + v.(string))
			fmt.Printf("in multi, data %v\n", res)
		}
		out <- res
	}
}

func CombineResults(in, out chan interface{}) {
	var combo []string
	for v := range in {
		//fmt.Printf("in combo, data %v\n", v)
		combo = append(combo, v.(string))
	}
	sort.Strings(combo)
	res := combo[0]
	for i := 1; i < len(combo); i++ {
		res += "_" + combo[i]
	}
	//fmt.Printf("in combo, res %v\n", res)
	out <- res
}
