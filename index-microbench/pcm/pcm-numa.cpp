/*
   Copyright (c) 2009-2013, Intel Corporation
   All rights reserved.

   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of Intel Corporation nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
// written by Roman Dementiev


/*!     \file pcm-numa.cpp
  \brief Example of using CPU counters: implements a performance counter monitoring utility for NUMA (remote and local memory accesses counting). Example for programming offcore response events
*/
#define HACK_TO_REMOVE_DUPLICATE_ERROR
#include <iostream>
#ifdef _MSC_VER
#include <windows.h>
#include "../PCM_Win/windriver.h"
#else
#include <unistd.h>
#include <signal.h>
#include <sys/time.h> // for gettimeofday()
#endif
#include <math.h>
#include <iomanip>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <assert.h>
#include "cpucounters.h"
#include "utils.h"
#ifdef _MSC_VER
#include "freegetopt/getopt.h"
#endif

#include <vector>

using namespace std;

namespace PCM_NUMA {

template <class StateType>
void print_stats(const StateType & BeforeState, const StateType & AfterState) {
    uint64 cycles = getCycles(BeforeState, AfterState);
    uint64 instr = getInstructionsRetired(BeforeState, AfterState);

    cout << double(instr) / double(cycles) << "       ";
    cout << unit_format(instr) << "     ";
    cout << unit_format(cycles) << "      ";

    for (int i = 0; i < 2; ++i) {
      cout << unit_format(getNumberOfCustomEvents(i, BeforeState, AfterState)) << "              ";
    }

    cout << "\n";
}

static uint32_t ncores;
static uint64 BeforeTime = 0, AfterTime = 0;
static SystemCounterState SysBeforeState, SysAfterState;
static std::vector<CoreCounterState> BeforeState, AfterState;
static std::vector<SocketCounterState> DummySocketStates;

static PCM *m = nullptr;

void InitNUMAMonitor() {
    m = PCM::getInstance();

    EventSelectRegister def_event_select_reg;
    def_event_select_reg.value = 0;
    def_event_select_reg.fields.usr = 1;
    def_event_select_reg.fields.os = 1;
    def_event_select_reg.fields.enable = 1;

    PCM::ExtendedCustomCoreEventDescription conf;
    conf.fixedCfg = NULL; // default
    conf.nGPCounters = 4;
    switch (m->getCPUModel())
    {
    case PCM::WESTMERE_EX:
        conf.OffcoreResponseMsrValue[0] = 0x40FF;                // OFFCORE_RESPONSE.ANY_REQUEST.LOCAL_DRAM:  Offcore requests satisfied by the local DRAM
        conf.OffcoreResponseMsrValue[1] = 0x20FF;                // OFFCORE_RESPONSE.ANY_REQUEST.REMOTE_DRAM: Offcore requests satisfied by a remote DRAM
        break;
    case PCM::JAKETOWN:
    case PCM::IVYTOWN:
        conf.OffcoreResponseMsrValue[0] = 0x780400000 | 0x08FFF; // OFFCORE_RESPONSE.*.LOCAL_DRAM
        conf.OffcoreResponseMsrValue[1] = 0x7ff800000 | 0x08FFF; // OFFCORE_RESPONSE.*.REMOTE_DRAM
        break;
    case PCM::HASWELLX:
        conf.OffcoreResponseMsrValue[0] = 0x600400000 | 0x08FFF; // OFFCORE_RESPONSE.*.LOCAL_DRAM
        conf.OffcoreResponseMsrValue[1] = 0x63f800000 | 0x08FFF; // OFFCORE_RESPONSE.*.REMOTE_DRAM
        break;
    case PCM::BDX:
        conf.OffcoreResponseMsrValue[0] = 0x0604008FFF; // OFFCORE_RESPONSE.*.LOCAL_DRAM
        conf.OffcoreResponseMsrValue[1] = 0x067BC08FFF; // OFFCORE_RESPONSE.*.REMOTE_DRAM
        break;
    default:
        cerr << "pcm-numa tool does not support your processor currently." << endl;
        exit(EXIT_FAILURE);
    }

    EventSelectRegister regs[4];
    conf.gpCounterCfg = regs;
    for (int i = 0; i < 4; ++i)
        regs[i] = def_event_select_reg;

    regs[0].fields.event_select = 0xB7; // OFFCORE_RESPONSE 0 event
    regs[0].fields.umask = 0x01;
    regs[1].fields.event_select = 0xBB; // OFFCORE_RESPONSE 1 event
    regs[1].fields.umask = 0x01;

    PCM::ErrorCode status = m->program(PCM::EXT_CUSTOM_CORE_EVENTS, &conf);
    switch (status)
    {
    case PCM::Success:
        break;
    case PCM::MSRAccessDenied:
        cerr << "Access to Processor Counter Monitor has denied (no MSR or PCI CFG space access)." << endl;
        exit(EXIT_FAILURE);
    case PCM::PMUBusy:
        cerr << "Access to Processor Counter Monitor has denied (Performance Monitoring Unit is occupied by other application). Try to stop the application that uses PMU." << endl;
        cerr << "Alternatively you can try to reset PMU configuration at your own risk. Try to reset? (y/n)" << endl;
        char yn;
        std::cin >> yn;
        if ('y' == yn)
        {
            m->resetPMU();
            cerr << "PMU configuration has been reset. Try to rerun the program again." << endl;
        }
        exit(EXIT_FAILURE);
    default:
        cerr << "Access to Processor Counter Monitor has denied (Unknown error)." << endl;
        exit(EXIT_FAILURE);
    }

    ncores = m->getNumCores();

    return;
}

void StartNUMAMonitor() {
    BeforeTime = m->getTickCount();
    m->getAllCounterStates(SysBeforeState, DummySocketStates, BeforeState);

    return;
}

void EndNUMAMonitor() {
    AfterTime = m->getTickCount();
    m->getAllCounterStates(SysAfterState, DummySocketStates, AfterState);

    cout << "Core | IPC  | Instructions | Cycles  |  Local DRAM accesses | Remote DRAM Accesses \n";

    for (uint32 i = 0; i < ncores; ++i) {
      cout << " " << setw(3) << i << "   " << setw(2);
      print_stats(BeforeState[i], AfterState[i]);
    }


    cout << "-------------------------------------------------------------------------------------------------------------------\n";
    cout << "   *   ";

    print_stats(SysBeforeState, SysAfterState);
    std::cout << std::endl;

    return;
}

} // namespace 
