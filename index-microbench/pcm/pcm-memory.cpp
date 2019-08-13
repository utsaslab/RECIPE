/*

   Copyright (c) 2009-2017, Intel Corporation
   All rights reserved.

   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of Intel Corporation nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
// written by Patrick Lu
// increased max sockets to 256 - Thomas Willhalm


/*!     \file pcm-memory.cpp
  \brief Example of using CPU counters: implements a performance counter monitoring utility for memory controller channels and DIMMs (ranks)
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

//Programmable iMC counter
#define READ 0
#define WRITE 1
#define READ_RANK_A 0
#define WRITE_RANK_A 1
#define READ_RANK_B 2
#define WRITE_RANK_B 3
#define PARTIAL 2
#define PCM_DELAY_DEFAULT 1.0 // in seconds
#define PCM_DELAY_MIN 0.015 // 15 milliseconds is practical on most modern CPUs
#define PCM_CALIBRATION_INTERVAL 50 // calibrate clock only every 50th iteration

#define DEFAULT_DISPLAY_COLUMNS 2

using namespace std;

namespace PCM_memory {

const uint32 max_sockets = 256;
const uint32 max_imc_channels = 8;
const uint32 max_edc_channels = 8;

typedef struct memdata {
    float iMC_Rd_socket_chan[max_sockets][max_imc_channels];
    float iMC_Wr_socket_chan[max_sockets][max_imc_channels];
    float iMC_Rd_socket[max_sockets];
    float iMC_Wr_socket[max_sockets];
    float EDC_Rd_socket_chan[max_sockets][max_edc_channels];
    float EDC_Wr_socket_chan[max_sockets][max_edc_channels];
    float EDC_Rd_socket[max_sockets];
    float EDC_Wr_socket[max_sockets];
    uint64 partial_write[max_sockets];
} memdata_t;

void printSocketBWHeader(uint32 no_columns, uint32 skt)
{
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|---------------------------------------|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|--             Socket "<<setw(2)<<i<<"             --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|---------------------------------------|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|--     Memory Channel Monitoring     --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|---------------------------------------|";
    }
    cout << endl;
}

void printSocketRankBWHeader(uint32 no_columns, uint32 skt)
{
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|-------------------------------------------|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|--               Socket "<<setw(2)<<i<<"               --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|-------------------------------------------|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|--           DIMM Rank Monitoring        --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|-------------------------------------------|";
    }
    cout << endl;
}

void printSocketChannelBW(PCM *m, memdata_t *md, uint32 no_columns, uint32 skt)
{
    for (uint32 channel = 0; channel < max_imc_channels; ++channel) {
        // check all the sockets for bad channel "channel"
        unsigned bad_channels = 0;
        for (uint32 i=skt; i<(skt+no_columns); ++i) {
            if (md->iMC_Rd_socket_chan[i][channel] < 0.0 || md->iMC_Wr_socket_chan[i][channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
                ++bad_channels;
        }
        if (bad_channels == no_columns) { // the channel is missing on all sockets in the row
            continue;
        }
        for (uint32 i=skt; i<(skt+no_columns); ++i) {
            cout << "|-- Mem Ch "<<setw(2)<<channel<<": Reads (MB/s): "<<setw(8)<<md->iMC_Rd_socket_chan[i][channel]<<" --|";
        }
        cout << endl;
        for (uint32 i=skt; i<(skt+no_columns); ++i) {
            cout << "|--            Writes(MB/s): "<<setw(8)<<md->iMC_Wr_socket_chan[i][channel]<<" --|";
        }
        cout << endl;
    }
}

void printSocketChannelBW(uint32 no_columns, uint32 skt, uint32 num_imc_channels, const ServerUncorePowerState * uncState1, const ServerUncorePowerState * uncState2, uint64 elapsedTime, int rankA, int rankB)
{
    for (uint32 channel = 0; channel < num_imc_channels; ++channel) {
        if(rankA >= 0) {
          for (uint32 i=skt; i<(skt+no_columns); ++i) {
              cout << "|-- Mem Ch "<<setw(2)<<channel<<" R " << setw(1) << rankA <<": Reads (MB/s): "<<setw(8)<<(float) (getMCCounter(channel,READ_RANK_A,uncState1[i],uncState2[i]) * 64 / 1000000.0 / (elapsedTime/1000.0))<<" --|";
          }
          cout << endl;
          for (uint32 i=skt; i<(skt+no_columns); ++i) {
              cout << "|--                Writes(MB/s): "<<setw(8)<<(float) (getMCCounter(channel,WRITE_RANK_A,uncState1[i],uncState2[i]) * 64 / 1000000.0 / (elapsedTime/1000.0))<<" --|";
          }
          cout << endl;
        }
        if(rankB >= 0) {
          for (uint32 i=skt; i<(skt+no_columns); ++i) {
              cout << "|-- Mem Ch "<<setw(2) << channel<<" R " << setw(1) << rankB <<": Reads (MB/s): "<<setw(8)<<(float) (getMCCounter(channel,READ_RANK_B,uncState1[i],uncState2[i]) * 64 / 1000000.0 / (elapsedTime/1000.0))<<" --|";
          }
          cout << endl;
          for (uint32 i=skt; i<(skt+no_columns); ++i) {
              cout << "|--                Writes(MB/s): "<<setw(8)<<(float) (getMCCounter(channel,WRITE_RANK_B,uncState1[i],uncState2[i]) * 64 / 1000000.0 / (elapsedTime/1000.0))<<" --|";
          }
          cout << endl;
        }
    }
}

void printSocketBWFooter(uint32 no_columns, uint32 skt, float* iMC_Rd_socket, float* iMC_Wr_socket, uint64* partial_write)
{
    for (uint32 i=skt; i<(skt+no_columns); ++i) {
        cout << "|-- NODE"<<setw(2)<<i<<" Mem Read (MB/s) : "<<setw(8)<<iMC_Rd_socket[i]<<" --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(skt+no_columns); ++i) {
        cout << "|-- NODE"<<setw(2)<<i<<" Mem Write(MB/s) : "<<setw(8)<<iMC_Wr_socket[i]<<" --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(skt+no_columns); ++i) {
        cout << "|-- NODE"<<setw(2)<<i<<" P. Write (T/s): "<<dec<<setw(10)<<partial_write[i]<<" --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(skt+no_columns); ++i) {
        cout << "|-- NODE"<<setw(2)<<i<<" Memory (MB/s): "<<setw(11)<<std::right<<iMC_Rd_socket[i]+iMC_Wr_socket[i]<<" --|";
    }
    cout << endl;
    for (uint32 i=skt; i<(no_columns+skt); ++i) {
        cout << "|---------------------------------------|";
    }
    cout << endl;
}

void display_bandwidth(PCM *m, memdata_t *md, uint32 no_columns)
{
    float sysRead = 0.0, sysWrite = 0.0;
    uint32 numSockets = m->getNumSockets();
    uint32 skt = 0;
    cout.setf(ios::fixed);
    cout.precision(2);

    while(skt < numSockets)
    {
        // Full row
        if ( (skt+no_columns) <= numSockets )
        {
            printSocketBWHeader (no_columns, skt);
            printSocketChannelBW(m, md, no_columns, skt);
            printSocketBWFooter (no_columns, skt, md->iMC_Rd_socket, md->iMC_Wr_socket, md->partial_write);
            for (uint32 i=skt; i<(skt+no_columns); i++) {
                sysRead += md->iMC_Rd_socket[i];
                sysWrite += md->iMC_Wr_socket[i];
            }
            skt += no_columns;
        }
        else //Display one socket in this row
        {
            if (m->MCDRAMmemoryTrafficMetricsAvailable())
            {
                cout << "\
                    \r|---------------------------------------||---------------------------------------|\n\
                    \r|--                              Processor socket " << skt << "                            --|\n\
                    \r|---------------------------------------||---------------------------------------|\n\
                    \r|--       DDR4 Channel Monitoring     --||--      MCDRAM Channel Monitoring    --|\n\
                    \r|---------------------------------------||---------------------------------------|\n\
                    \r";
                uint32 max_channels = max_imc_channels <= max_edc_channels ? max_edc_channels : max_imc_channels;
                float iMC_Rd, iMC_Wr, EDC_Rd, EDC_Wr;
                for(uint64 channel = 0; channel < max_channels; ++channel)
                {
                    if (channel <= max_imc_channels) {
                        iMC_Rd = md->iMC_Rd_socket_chan[skt][channel];
                        iMC_Wr = md->iMC_Wr_socket_chan[skt][channel];
		    }
		    else
		    {
		    	iMC_Rd = -1.0;
		    	iMC_Wr = -1.0;
		    }
		    if (channel <= max_edc_channels) {
                        EDC_Rd = md->EDC_Rd_socket_chan[skt][channel];
                        EDC_Wr = md->EDC_Wr_socket_chan[skt][channel];
		    }
		    else
		    {
		    	EDC_Rd = -1.0;
		    	EDC_Rd = -1.0;
		    }

		    if (iMC_Rd >= 0.0 && iMC_Wr >= 0.0 && EDC_Rd >= 0.0 && EDC_Wr >= 0.0)
		    	cout << "|-- DDR4 Ch " << channel <<": Reads (MB/s):" << setw(9)  << iMC_Rd
		    	     << " --||-- EDC Ch " << channel <<": Reads (MB/s):" << setw(10)  << EDC_Rd
		    	     << " --|\n|--            Writes(MB/s):" << setw(9) << iMC_Wr
		    	     << " --||--           Writes(MB/s):" << setw(10)  << EDC_Wr
		    	     <<" --|\n";
		    else if ((iMC_Rd < 0.0 || iMC_Wr < 0.0) && EDC_Rd >= 0.0 && EDC_Wr >= 0.0)
		    	cout << "|--                                  "
		    	     << " --||-- EDC Ch " << channel <<": Reads (MB/s):" << setw(10)  << EDC_Rd
		    	     << " --|\n|--                                  "
		    	     << " --||--           Writes(MB/s):" << setw(10)  << EDC_Wr
		    	     <<" --|\n";

		    else if (iMC_Rd >= 0.0 && iMC_Wr >= 0.0 && (EDC_Rd < 0.0 || EDC_Wr < 0.0))
		    	cout << "|-- DDR4 Ch " << channel <<": Reads (MB/s):" << setw(9)  << iMC_Rd
		    	     << " --||--                                  "
		    	     << " --|\n|--            Writes(MB/s):" << setw(9) << iMC_Wr
		    	     << " --||--                                  "
		    	     <<" --|\n";
		    else
		    	continue;
                }
                cout << "\
                    \r|-- DDR4 Mem Read  (MB/s):"<<setw(11)<<md->iMC_Rd_socket[skt]<<" --||-- MCDRAM Read (MB/s):"<<setw(14)<<md->EDC_Rd_socket[skt]<<" --|\n\
                    \r|-- DDR4 Mem Write (MB/s):"<<setw(11)<<md->iMC_Wr_socket[skt]<<" --||-- MCDRAM Write(MB/s):"<<setw(14)<<md->EDC_Wr_socket[skt]<<" --|\n\
                    \r|-- DDR4 Memory (MB/s)   :"<<setw(11)<<md->iMC_Rd_socket[skt]+md->iMC_Wr_socket[skt]<<" --||-- MCDRAM (MB/s)     :"<<setw(14)<<md->EDC_Rd_socket[skt]+md->EDC_Wr_socket[skt]<<" --|\n\
                    \r|---------------------------------------||---------------------------------------|\n\
                    \r";

                sysRead  += (md->iMC_Rd_socket[skt]+md->EDC_Rd_socket[skt]);
                sysWrite += (md->iMC_Wr_socket[skt]+md->EDC_Wr_socket[skt]);
                skt += 1;
            }
	    else
	    {
                cout << "\
                    \r|---------------------------------------|\n\
                    \r|--             Socket "<<skt<<"              --|\n\
                    \r|---------------------------------------|\n\
                    \r|--     Memory Channel Monitoring     --|\n\
                    \r|---------------------------------------|\n\
                    \r"; 
                for(uint64 channel = 0; channel < max_imc_channels; ++channel)
                {
                    if(md->iMC_Rd_socket_chan[skt][channel] < 0.0 && md->iMC_Wr_socket_chan[skt][channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
                        continue;
                    cout << "|--  Mem Ch " << channel <<": Reads (MB/s):" << setw(8)  << md->iMC_Rd_socket_chan[skt][channel]
                        <<"  --|\n|--            Writes(MB/s):" << setw(8) << md->iMC_Wr_socket_chan[skt][channel]
                        <<"  --|\n";
                }
                cout << "\
                    \r|-- NODE"<<skt<<" Mem Read (MB/s):  "<<setw(8)<<md->iMC_Rd_socket[skt]<<"  --|\n\
                    \r|-- NODE"<<skt<<" Mem Write (MB/s) :"<<setw(8)<<md->iMC_Wr_socket[skt]<<"  --|\n\
                    \r|-- NODE"<<skt<<" P. Write (T/s) :"<<setw(10)<<dec<<md->partial_write[skt]<<"  --|\n\
                    \r|-- NODE"<<skt<<" Memory (MB/s): "<<setw(8)<<md->iMC_Rd_socket[skt]+md->iMC_Wr_socket[skt]<<"     --|\n\
                    \r|---------------------------------------|\n\
                    \r";

                sysRead += md->iMC_Rd_socket[skt];
                sysWrite += md->iMC_Wr_socket[skt];
                skt += 1;
            }
        }
        cout << "\
            \r|---------------------------------------||---------------------------------------|\n\
            \r|--                 System Read Throughput(MB/s):"<<setw(14)<<sysRead<<"                --|\n\
            \r|--                System Write Throughput(MB/s):"<<setw(14)<<sysWrite<<"                --|\n\
            \r|--               System Memory Throughput(MB/s):"<<setw(14)<<sysRead+sysWrite<<"                --|\n\
            \r|---------------------------------------||---------------------------------------|" << endl;
    }
}

void calculate_bandwidth(PCM *m, const ServerUncorePowerState uncState1[], const ServerUncorePowerState uncState2[], uint64 elapsedTime, uint32 no_columns)
{
    //const uint32 num_imc_channels = m->getMCChannelsPerSocket();
    //const uint32 num_edc_channels = m->getEDCChannelsPerSocket();
    memdata_t md;

    for(uint32 skt = 0; skt < m->getNumSockets(); ++skt)
    {
        md.iMC_Rd_socket[skt] = 0.0;
        md.iMC_Wr_socket[skt] = 0.0;
        md.EDC_Rd_socket[skt] = 0.0;
        md.EDC_Wr_socket[skt] = 0.0;
        md.partial_write[skt] = 0;

	switch(m->getCPUModel()) {
	case PCM::KNL:
            for(uint32 channel = 0; channel < max_edc_channels; ++channel)
            {
                if(getEDCCounter(channel,READ,uncState1[skt],uncState2[skt]) == 0.0 && getEDCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) == 0.0)
                {
                    md.EDC_Rd_socket_chan[skt][channel] = -1.0;
                    md.EDC_Wr_socket_chan[skt][channel] = -1.0;
                    continue;
                }

                md.EDC_Rd_socket_chan[skt][channel] = (float) (getEDCCounter(channel,READ,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));
                md.EDC_Wr_socket_chan[skt][channel] = (float) (getEDCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));

                md.EDC_Rd_socket[skt] += md.EDC_Rd_socket_chan[skt][channel];
                md.EDC_Wr_socket[skt] += md.EDC_Wr_socket_chan[skt][channel];
	    }
        default:
            for(uint32 channel = 0; channel < max_imc_channels; ++channel)
            {
                if(getMCCounter(channel,READ,uncState1[skt],uncState2[skt]) == 0.0 && getMCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) == 0.0) //In case of JKT-EN, there are only three channels. Skip one and continue.
                {
                    md.iMC_Rd_socket_chan[skt][channel] = -1.0;
                    md.iMC_Wr_socket_chan[skt][channel] = -1.0;
                    continue;
                }

                md.iMC_Rd_socket_chan[skt][channel] = (float) (getMCCounter(channel,READ,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));
                md.iMC_Wr_socket_chan[skt][channel] = (float) (getMCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));

                md.iMC_Rd_socket[skt] += md.iMC_Rd_socket_chan[skt][channel];
                md.iMC_Wr_socket[skt] += md.iMC_Wr_socket_chan[skt][channel];

                md.partial_write[skt] += (uint64) (getMCCounter(channel,PARTIAL,uncState1[skt],uncState2[skt]) / (elapsedTime/1000.0));
            }
	}
    }

    display_bandwidth(m, &md, no_columns);
}

// Static global variables defined for monitoring memory bandwidth.
static PCM *m = nullptr;
static ServerUncorePowerState *BeforeState = nullptr;
static ServerUncorePowerState *AfterState = nullptr;

static uint64_t BeforeTime = 0UL, AfterTime = 0UL;

void InitMemoryMonitor() {
    m = PCM::getInstance();
    fprintf(stderr, "After getting PCM instance\n");
    m->disableJKTWorkaround();
    PCM::ErrorCode status = m->programServerUncoreMemoryMetrics(-1, -1);
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
    
    if(!m->hasPCICFGUncore())
    {
        cerr << "Jaketown, Ivytown or Haswell Server CPU is required for this tool!" << endl;
        if(m->memoryTrafficMetricsAvailable())
            cerr << "For processor-level memory bandwidth statistics please use pcm.x" << endl;
        exit(EXIT_FAILURE);
    }

    if(m->getNumSockets() > max_sockets)
    {
        cerr << "Only systems with up to "<<max_sockets<<" sockets are supported! Program aborted" << endl;
        exit(EXIT_FAILURE);
    }

    return;
}

void StartMemoryMonitor() {
    // Allocate memory and store them using the statuc global variable.
    // If the end routine is not called then we have a memory leak
    BeforeState = new ServerUncorePowerState[m->getNumSockets()];
    AfterState = new ServerUncorePowerState[m->getNumSockets()];

    for(uint32 i=0; i<m->getNumSockets(); ++i) {
        BeforeState[i] = m->getServerUncorePowerState(i); 
    }

    BeforeTime = m->getTickCount();

    return;
}

void EndMemoryMonitor() {
    AfterTime = m->getTickCount();
    for(uint32 i=0; i<m->getNumSockets(); ++i) {
      AfterState[i] = m->getServerUncorePowerState(i);
    }

    calculate_bandwidth(m, BeforeState, AfterState, AfterTime-BeforeTime, 2);

    delete[] BeforeState;
    delete[] AfterState;

    return;
}


#undef READ
#undef WRITE
#undef READ_RANK_A
#undef WRITE_RANK_A
#undef READ_RANK_B
#undef WRITE_RANK_B
#undef PARTIAL

} // namespace PCM_memory
