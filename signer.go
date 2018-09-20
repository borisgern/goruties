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

type result struct {
	res      string
	allCrc32 map[int]string
	i        int
}

type results map[string]*result

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
	wgLeft := sync.WaitGroup{}
	wgRight := sync.WaitGroup{}
	mu := &sync.Mutex{}
	left := make(chan interface{})
	right := make(chan interface{})
	done := make(chan interface{})
	wg := sync.WaitGroup{}
	syncChan := make(chan struct{})

	go func() {
		for v := range in {
			fmt.Printf("%v SingleHash data %[1]v\n", v)
			left <- v
			right <- v
		}
		close(left)
		close(right)
	}()

	go func() {
		for v := range left {
			wgLeft.Add(1)
			go func(v int) {
				defer wgLeft.Done()
				data := halfHash{parent: v, val: DataSignerCrc32(strconv.Itoa(v)), step: leftStep}
				fmt.Printf("%v SingleHash crc32(data) %v\n", v, data.val)
				done <- data
			}(v.(int))
		}
		wgLeft.Wait()
		syncChan <- struct{}{}
	}()

	go func() {
		for v := range right {
			data := halfHash{parent: v.(int), val: DataSignerMd5(strconv.Itoa(v.(int))), step: rightMd5Step}
			fmt.Printf("%v SingleHash md5(data) %v\n", v, data.val)
			wgRight.Add(1)
			go func(md5 halfHash, v int) {
				defer wgRight.Done()
				data := halfHash{parent: md5.parent, val: DataSignerCrc32(md5.val), step: rightCrc32Step}
				fmt.Printf("%v crc32(md5(data)) %v\n", v, data.val)
				done <- data
			}(data, v.(int))
		}
		wgRight.Wait()
		syncChan <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range done {
			if data.(halfHash).step == leftStep {
				mu.Lock()
				if val, ok := singleHash[data.(halfHash).parent]; !ok {
					singleHash[data.(halfHash).parent] = data.(halfHash).val
				} else if data.(halfHash).val != val {
					out <- val + "~" + data.(halfHash).val
				}
				mu.Unlock()
			} else {
				mu.Lock()
				if val, ok := singleHash[data.(halfHash).parent]; !ok {
					singleHash[data.(halfHash).parent] = data.(halfHash).val
				} else if data.(halfHash).val != val {
					fmt.Printf("%v SingleHash result %v\n", data.(halfHash).parent, val+"~"+data.(halfHash).val)
					out <- val + "~" + data.(halfHash).val
				}
				mu.Unlock()
			}
		}
	}()

	go func() {
		<-syncChan
		<-syncChan
		close(done)
	}()

	wg.Wait()

}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	done := make(chan interface{})
	wgAllCrc := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	allRes := make(results)

	for v := range in {
		wgAllCrc.Add(1)
		go func(v string) {
			defer wgAllCrc.Done()
			wgCrc := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			for i := 0; i < 6; i++ {
				wgCrc.Add(1)
				go func(i int, v string, mu *sync.Mutex) {
					defer wgCrc.Done()
					data := halfMulti{parent: v, val: DataSignerCrc32(strconv.Itoa(i) + v), step: i}
					fmt.Printf("%v MultiHash: crc32(th+step1)) %v %v\n", v, i, data.val)
					done <- data
				}(i, v, mu)
			}
			wgCrc.Wait()
		}(v.(string))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		wgAllCrc.Wait()
		close(done)
	}()

	for data := range done {
		wg.Add(1)
		go func(data halfMulti) {
			defer wg.Done()
			mu.Lock()
			if val, ok := allRes[data.parent]; !ok {
				allRes[data.parent] = &result{allCrc32: make(map[int]string, 6)}
				allRes[data.parent].allCrc32[data.step] = data.val
				allRes[data.parent].i++
			} else {
				val.allCrc32[data.step] = data.val
				val.i++
			}
			if allRes[data.parent].i == 6 {
				for i := 0; i < 6; i++ {
					allRes[data.parent].res += allRes[data.parent].allCrc32[i]
				}
				fmt.Printf("%v MultiHash result:\n%v\n", data.parent, allRes[data.parent].res)
				out <- allRes[data.parent].res
				delete(allRes, data.parent)
			}
			mu.Unlock()
		}(data.(halfMulti))
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var combo []string
	for v := range in {
		combo = append(combo, v.(string))
	}
	sort.Strings(combo)
	res := combo[0]
	for i := 1; i < len(combo); i++ {
		res += "_" + combo[i]
	}
	fmt.Printf("CombineResults\n%v\n", res)
	out <- res
}
