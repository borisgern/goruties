package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

const (
	leftStep       = 1
	rightMd5Step   = 2
	rightCrc32Step = 3
)

type halfHash struct {
	parent int
	val    string
	step   int
}

type halfMulti struct {
	parent string
	val    string
	step   int
}

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

func SingleHash(in, out chan interface{}) {
	singleHash := make(map[int]string)
	wgLeft := &sync.WaitGroup{}
	wgRight := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	left := make(chan interface{})
	right := make(chan interface{})
	done := make(chan interface{})
	wg := &sync.WaitGroup{}
	var i int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range in {
			//fmt.Printf("step %v in single with %v\n", i, v)
			i++
			//fmt.Printf("send %v to left\n", v)
			left <- v
			//fmt.Printf("send %v to right\n", v)
			right <- v
		}
		close(left)
		close(right)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range left {
			//fmt.Printf("left get %v from in\n", v)
			wgLeft.Add(1)
			go func(v int) {
				defer wgLeft.Done()
				//fmt.Printf("start left DataSignerCrc32 for %v\n", v)
				data := halfHash{parent: v, val: DataSignerCrc32(strconv.Itoa(v)), step: leftStep}
				//fmt.Printf("send %v to done from left\n", data)
				done <- data
			}(v.(int))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range right {
			//fmt.Printf("right get %v from in\n", v)
			//fmt.Printf("start right DataSignerMd5 for %v\n", v)
			data := halfHash{parent: v.(int), val: DataSignerMd5(strconv.Itoa(v.(int))), step: rightMd5Step}
			//fmt.Printf("send %v to rightCrc32\n", data)
			wgRight.Add(1)
			go func(md5 halfHash) {
				defer wgRight.Done()
				//fmt.Printf("start right DataSignerCrc32 for %v\n", v)
				data := halfHash{parent: md5.parent, val: DataSignerCrc32(md5.val), step: rightCrc32Step}
				//fmt.Printf("send %v to done from right\n", data)
				done <- data
			}(data)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range done {
			//fmt.Printf("value of data is %v\n", data)

			if data.(halfHash).step == leftStep {
				mu.Lock()
				if val, ok := singleHash[data.(halfHash).parent]; !ok {
					//	fmt.Printf("singleHash[%v.(halfHash).%v] is %v, ok is %v\n", data, data.(halfHash).parent, val, ok)
					singleHash[data.(halfHash).parent] = data.(halfHash).val
				} else if data.(halfHash).val != val {
					//	fmt.Printf("singleHash[%v.(halfHash).%v] is %v, ok is %v\n", data, data.(halfHash).parent, val, ok)
					//	fmt.Printf("single res %v\n", val+"~"+data.(halfHash).val)
					out <- val + "~" + data.(halfHash).val

				}
				mu.Unlock()
			} else {
				mu.Lock()
				if val, ok := singleHash[data.(halfHash).parent]; !ok {
					//	fmt.Printf("singleHash[%v.(halfHash).%v] is %v, ok is %v\n", data, data.(halfHash).parent, val, ok)
					singleHash[data.(halfHash).parent] = data.(halfHash).val
				} else if data.(halfHash).val != val {
					//	fmt.Printf("singleHash[%v.(halfHash).%v] is %v, ok is %v\n", data, data.(halfHash).parent, val, ok)
					//	fmt.Printf("single res %v\n", val+"~"+data.(halfHash).val)
					out <- val + "~" + data.(halfHash).val
				}
				mu.Unlock()
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		wgLeft.Wait()
		wgRight.Wait()
		close(done)
	}()
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	done := make(chan interface{})
	wgAllCrc := &sync.WaitGroup{}

	var i int

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	for v := range in {
		//fmt.Printf("step %v in multi with %v\n", i, v)
		i++
		wgAllCrc.Add(1)
		go func(v string) {
			defer wgAllCrc.Done()
			wgCrc := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			//fmt.Printf("start loop of 6 for %v\n", v)
			for i := 0; i < 6; i++ {
				wgCrc.Add(1)
				go func(i int, v string, mu *sync.Mutex) {
					defer wgCrc.Done()
					//fmt.Printf("start crc32 for %v and %v\n", v, i)
					data := halfMulti{parent: v, val: DataSignerCrc32(strconv.Itoa(i) + v), step: i}
					//fmt.Printf("send %v to done\n", data)
					done <- data
				}(i, v, mu)
			}
			wgCrc.Wait()
		}(v.(string))
	}
	//}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		wgAllCrc.Wait()
		fmt.Printf("!!!!!! closing done\n")
		close(done)
	}()

	type result struct {
		res      string
		allCrc32 map[int]string
		i        int
		parent   string
		multiple int
	}

	type results map[string]*result

	mu := &sync.Mutex{}
	allRes := make(results)
	//wgRes := &sync.WaitGroup{}
	for data := range done {
		wg.Add(1)
		//wgRes.Add(1)
		//fmt.Printf("multi data %v\n", data)
		go func(data halfMulti) {
			defer wg.Done()
			//defer wgRes.Done()
			mu.Lock()
			if val, ok := allRes[data.parent]; !ok {
				allRes[data.parent] = &result{parent: data.parent, allCrc32: make(map[int]string, 6), multiple: 1}
				allRes[data.parent].allCrc32[data.step] = data.val
				allRes[data.parent].i++
			} else {
				// if allRes[data.parent].parent == data.parent {
				// 	allRes[data.parent].multiple++
				// 	fmt.Printf("parent %v multiple is %v\n", data.parent, allRes[data.parent].multiple)
				// }
				val.allCrc32[data.step] = data.val
				val.i++
			}

			if allRes[data.parent].i == 6 {
				for i := 0; i < 6; i++ {
					//fmt.Printf("allRes[data.parent] %v data %v\n", allRes[data.parent], data)
					allRes[data.parent].res += allRes[data.parent].allCrc32[i]
					//fmt.Printf("in multi, data %v\n", allRes[data.parent].res)
				}
				fmt.Printf("allRes[data.parent].multiple %v, allRes[data.parent].parent %v\n", allRes[data.parent].multiple, allRes[data.parent].parent)
				//for i := 0; i < allRes[data.parent].multiple/6; i++ {
				fmt.Printf("multi res %v\n", allRes[data.parent].res)
				out <- allRes[data.parent].res
				delete(allRes, data.parent)
				//}

			}
			mu.Unlock()
		}(data.(halfMulti))

		//fmt.Printf("allRes[data.parent].multiple %v, allRes[data.parent].parent %v\n", allRes[data.parent].multiple, allRes[data.parent].parent)

	}
	// wgRes.Wait()
	// go func() {
	// 	for _, v := range allRes {
	// 		mult := v.multiple / 6
	// 		var res string
	// 		for i := 0; i < 6; i++ {
	// 			//fmt.Printf("allRes[data.parent] %v data %v\n", allRes[data.parent], data)
	// 			res += v.allCrc32[i]
	// 			//fmt.Printf("in multi, data %v\n", allRes[data.parent].res)
	// 		}
	// 		for m := 0; i < mult; m++ {
	// 			fmt.Printf("multi res %v\n", res)
	// 			out <- res
	// 		}
	// 	}
	// }()

	wg.Wait()

}

func CombineResults(in, out chan interface{}) {
	var combo []string
	var i int
	for v := range in {
		fmt.Printf("step %v in combo with %v\n", i, v)
		i++
		fmt.Printf("in combo, data %v\n", v)
		combo = append(combo, v.(string))
	}
	sort.Strings(combo)
	res := combo[0]
	for i := 1; i < len(combo); i++ {
		res += "_" + combo[i]
	}
	fmt.Printf("in combo, res %v\n", res)
	out <- res
}
