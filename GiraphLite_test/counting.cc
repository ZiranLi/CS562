/**
 * @file PageRankVertex.cc
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @section DESCRIPTION
 *
 * This file implements the PageRank algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <iostream>
#include <set>
#include <map>



#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) PageRankVertex##name

#define EPS 1e-6

using namespace std;


//static  int through = 0;
//static  int cycle = 0;


struct A {
    int source;
    int type;
    int value;
};

struct B {
    int through;
    int cycle;
};




void Intersection(set<int> &A,set<int> &B,set<int> &result){
    set<int>::iterator it;

    it = A.begin();
    while(it != A.end()){
        if(B.find(*it) != B.end()) result.insert(*it);
        it++;
    }
}

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(B);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(A);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;

        double value = 1;
        int outdegree = 0;

        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        B value;
        char s[1024];

        int tmp_through;
        int tmp_cycle;

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            tmp_through += value.through;
            tmp_cycle += value.cycle;
        }

        //cout<<"through"<<tmp_through<<endl;
        //cout<<"cycle"<<tmp_cycle<<endl;
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (double *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (double *)p;
    }
    void accumulate(const void* p) {
        m_local += * (double *)p;
    }
};


/*
class VERTEX_CLASS_NAME(): public Vertex <double, double, A> {
public:
    void compute(MessageIterator* pmsgs) {
        double val;
        vector<int> tmp(3,2);
        A tmp_msg = {0};

        for (int i = 0; i < tmp.size(); i++) {
            cout << tmp[i] << endl;
        }

        if (getSuperstep() == 0)
        {
            val= 1.0;
        }
        else
        {
            if (getSuperstep() >= 2) {
                double global_val = * (double *)getAggrGlobal(0);
                if (global_val < EPS) {
                    voteToHalt(); return;
                }
            }

            double sum = 0;
            for ( ; ! pmsgs->done(); pmsgs->next() )
            {
                sum += pmsgs->getValue().value;
            }
            val = 0.15 + 0.85 * sum;

            double acc = fabs(getValue() - val);
            accumulateAggr(0, &acc);
        }
        * mutableValue() = val;
        const int64_t n = getOutEdgeIterator().size();
        tmp_msg.value =  val / n;
        sendMessageToAllNeighbors(tmp_msg);
    }
};


*/



class VERTEX_CLASS_NAME(): public Vertex  <B, double, A>
{

public:


public:
    void compute(MessageIterator *pmsgs)
    {

        set <int> inneighbor;
        set <int> outneighbor;
        map <int, set <int > > in_inneighbor;
        map <int, set <int > > in_outneighbor;

        set<int>::iterator itset;
        int tmp_show  = getVertexId();

        if (getSuperstep() == 0)
        {
            //send myself
            int source  = getVertexId();
            A tmp_msg = {source,0,source};
            sendMessageToAllNeighbors(tmp_msg);

            //get outneighbor
            OutEdgeIterator iter_edge = getOutEdgeIterator();
            for (;!iter_edge.done();iter_edge.next())
            {
                int tmp_outid = iter_edge.target();
                outneighbor.insert(tmp_outid);
            }


        }

        if (getSuperstep() == 1)
        {


            int source  = getVertexId();

            //send myself
            A tmp_msg = {source,0,source};
            sendMessageToAllNeighbors(tmp_msg);

            //get outneighbor
            OutEdgeIterator iter_edge = getOutEdgeIterator();
            for (;!iter_edge.done();iter_edge.next())
            {
                int tmp_outid = iter_edge.target();
                outneighbor.insert(tmp_outid);
            }

            //get inneighbor
            for ( ; ! pmsgs->done(); pmsgs->next() )
            {
                A tmp_msg_rcv = pmsgs->getValue();
                if (tmp_msg_rcv.type == 0)
                {
                    inneighbor.insert(tmp_msg_rcv.source);
                }
            }

	    /*
            cout<<source<<"inneighbor"<<endl;
            for (itset = inneighbor.begin();itset!=inneighbor.end();itset++)
            {
                cout<<*itset<<" ";
            }
            cout<<endl;
	    */	
	

            //send outneighbor
            for (itset = outneighbor.begin(); itset!=outneighbor.end();itset++)
            {
                int type = 2;
                int value = *itset;
                A tmp_msg_out = {source,type,value};
                sendMessageToAllNeighbors(tmp_msg_out);
            }

            //send inneighbor
            for (itset = inneighbor.begin(); itset!=inneighbor.end();itset++)
            {
                int type = 1;
                int value = *itset;
                A tmp_msg_in = {source,type,value};
                sendMessageToAllNeighbors(tmp_msg_in);
            }
        }

        if (getSuperstep() == 2)
        {
            int source_self = getVertexId();

            //get outneighbor
            OutEdgeIterator iter_edge = getOutEdgeIterator();
            for (;!iter_edge.done();iter_edge.next())
            {
                int tmp_outid = iter_edge.target();
                outneighbor.insert(tmp_outid);
            }


            for (; !pmsgs->done(); pmsgs->next())
            {
                int source, value;
                A tmp_msg_rcv = pmsgs->getValue();


                //get inneighbor
                if (tmp_msg_rcv.type == 0)
                {
                    inneighbor.insert(tmp_msg_rcv.source);
                }


                //get ininneighbor
                if (tmp_msg_rcv.type == 1) {
                    source = tmp_msg_rcv.source;
                    value = tmp_msg_rcv.value;
                    //map <int, set <int > > in_inneighbor;
                    if (in_inneighbor.find(source) == in_inneighbor.end()) {
                        set<int> tmp;
                        in_inneighbor[source] = tmp;
                        in_inneighbor[source].insert(value);
                    } else {
                        in_inneighbor[source].insert(value);
                    }
                }


                //gei outinneighbor
                if (tmp_msg_rcv.type == 2) {
                    source = tmp_msg_rcv.source;
                    value = tmp_msg_rcv.value;
                    //map <int, set <int > > in_inneighbor;
                    if (in_outneighbor.find(source) == in_outneighbor.end()) {
                        set<int> tmp;
                        in_outneighbor[source] = tmp;
                        in_outneighbor[source].insert(value);
                    } else {
                        in_outneighbor[source].insert(value);
                    }
                }


            }

            int64_t tmp_throuth = 0;
            for (itset = inneighbor.begin(); itset != inneighbor.end(); itset++)
            {
                int in_index = *itset;
                set<int> set_inout = in_outneighbor[in_index];
                //outneighbor
                set<int> c;

                set<int>::iterator itc = c.begin();
                Intersection(set_inout, outneighbor, c);
                int num_through = c.size();
                //through+=1;
                tmp_throuth += num_through;
            }
            //through+=tmp_throuth;

            //cout << "index "<<source_self <<" "<<tmp_throuth<<endl;

            int64_t tmp_cycle = 0;
            for (itset = inneighbor.begin(); itset != inneighbor.end(); itset++)
            {
                int in_index = *itset;
                set<int> set_inin = in_inneighbor[in_index];
                //outneighbor
                set<int> c;

                set<int>::iterator itc = c.begin();
                Intersection(set_inin, outneighbor, c);
                int num_cycle = c.size();
                //cycle+=1;
                tmp_cycle += num_cycle;

            }
            //cycle+=tmp_cycle;

            //cout << "index "<<source_self <<" "<<tmp_cycle<<endl;
            //B tmp_ver = {tmp_throuth,tmp_cycle};
            //* mutableValue() = tmp_ver;


            accumulateAggr(0, &tmp_throuth);
            accumulateAggr(1, &tmp_cycle);
        }

        if (getSuperstep() == 3)
        {

            int tmp_throuth = * (int *)getAggrGlobal(0);
            int tmp_cycle = * (int *)getAggrGlobal(1);

            //cout<<through<<endl;
            //cout<<cycle<<endl;
            cout<<"global_throuth"<<tmp_throuth << endl;
            cout<<"global_cycle"<<tmp_cycle << endl;


            voteToHalt();
            return;
        }

	/*
        cout<<tmp_show<<"outneighbor"<<endl;
        for (itset = outneighbor.begin();itset!=outneighbor.end();itset++)
        {
            cout<<*itset<<" ";
        }
        cout<<endl;

	*/

    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
            printf ("Usage: %s <input path> <output path>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[2];
        regNumAggr(2);
        regAggr(0, &aggregator[0]);
        regAggr(1, &aggregator[1]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
