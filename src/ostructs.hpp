#include <mpi.h>
#ifndef ostructs
#define ostructs
#define LONG_LONG_MAX __LONG_LONG_MAX__
#define LONG_LONG_MIN -LONG_LONG_MAX-1
#define TICKER_NAME_LENGTH 8
#define LINE_LOG_LENGTH 100000
#define DAY_VALUE 86400
#define MULTITHREAD_PARENT_OFF 0
#define MULTITHREAD_PARENT_ON 1
int rank;
int size;
struct datapoint{
    unsigned long long id=0;
    char name[TICKER_NAME_LENGTH]={' '};
    long long time;
    double open;
    double close;
    double adjClose;
    double change;
};
MPI_Datatype datapointDatatype;
struct ticker{
    unsigned long long id=0;
    int clusterStart;
    int clusterEnd;
    double mean;
    double std;
};
MPI_Datatype tickerDatatype;
struct analysisParameter{
    ticker base;
    double baseMin=__DBL_MAX__;
    double baseMax=__DBL_MIN__;
    long long baseDayDiff=0;

    ticker target;
    double targetMin=__DBL_MAX__;
    double targetMax=__DBL_MIN__;
    long long targetDayDiff=0;
    long long targetDayOffset;
    
    long long timeMin;
    long long timeMax;
    double zCutoff;
    double pCutoff;

    int baseInBound=0;
    int baseTotal=0;
    int targetInBound;
    int targetTotal;
};
MPI_Datatype analysisParameterDatatype;



#endif