package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/shirou/gopsutil/process"
)

func profileCPUAndMem(file string, DBname string) {
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var proc *process.Process
	for _ = range time.NewTicker(1 * time.Second).C {
		if proc == nil {
			procs, err := process.Processes()
			if err != nil {
				panic(err)
			}
			for _, p := range procs {
				cmd, _ := p.Cmdline()
				if strings.Contains(cmd, DBname) && (strings.Contains(cmd, "INSERT") || strings.Contains(cmd, "insert")) {
					proc = p
					break
				}
			}
		} else {
			cpu, err := proc.CPUPercent()
			if err != nil {
				proc = nil
				continue
			}
			mem, err := proc.MemoryInfo()
			if err != nil {
				proc = nil
				continue
			}

			fmt.Fprintf(f, "CPU Percent: %f, MemRSS: %d, MemVMS: %d, MemSwap：%d\n", cpu, mem.RSS, mem.VMS, mem.Swap)
		}
	}
}
