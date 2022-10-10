#ifndef oanalysis
#define oanalysis
#include "ostructs.hpp"
#include "oio.hpp"
#include "ologger.hpp"
#include "ostat.hpp"

void backtest(
    int MULTITHREAD_MODE,
    std::vector<ticker> *sourceTickers,
    std::vector<datapoint> *sourceDatapoints,
    std::vector<long long> *validDates,
    analysisParameter analysisTemplate,
    void analysis(int,std::vector<ticker> *,std::vector<datapoint> *,analysisParameter,std::vector<analysisParameter> *),
    double investment(int,long long,std::vector<ticker> *,std::vector<datapoint> *,std::vector<analysisParameter>,void analysis(int,std::vector<ticker> *,std::vector<datapoint> *,analysisParameter,std::vector<analysisParameter> *)),
    double baseProbMargin
){
    analysisTemplate.timeMin=validDates->at(0);
    int globalStart=0;
    int globalEnd=0;
    int localStart=0;
    int localEnd=0;
    int total = validDates->size();
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        if(total<size){
            globalStart=0;
            globalEnd=total;
        }else{
            int partition =total/size;
            localStart=partition*rank;
            localEnd= partition*(rank+1);
            globalStart=partition*(size);
            globalEnd=total;
        }
    }else{
        globalStart=0;
        globalEnd=total;
        MULTITHREAD_MODE=MULTITHREAD_PARENT_ON;
    }

    std::vector<double> invOUTLocal;
    for(int local=localStart;local<localEnd;local++){
        analysisTemplate.timeMax=validDates->at(local);
        std::vector<analysisParameter> apsIN;
        generateBaseBounds(validDates->at(local),&analysisTemplate,sourceTickers,sourceDatapoints,sourceTickers,baseProbMargin,&apsIN);
        double inv = investment(MULTITHREAD_MODE,validDates->at(local),sourceTickers,sourceDatapoints,apsIN,analysis);
        invOUTLocal.push_back(inv);
    }

    std::vector<double> invOUT;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localinvOUTCount = invOUTLocal.size();
        std::vector<int> localinvOUTCounts;
        localinvOUTCounts.resize(size);
        MPI_Allgather(&localinvOUTCount,1,MPI_INT,&localinvOUTCounts[0],1,MPI_INT,MPI_COMM_WORLD);
        std::vector<int> localinvOUTDispls;
        localinvOUTDispls.push_back(0);
        int totalinvOUT= localinvOUTCounts.at(0);
        for(int i=1;i<size;i++){
            totalinvOUT += localinvOUTCounts.at(i);
            localinvOUTDispls.push_back(localinvOUTDispls.at(i-1)+localinvOUTCounts.at(i-1));
        }
        invOUT.resize(totalinvOUT);
        
        MPI_Allgatherv(&invOUTLocal[0],localinvOUTCount,MPI_DOUBLE,(&invOUT[0]),&localinvOUTCounts[0],&localinvOUTDispls[0],MPI_DOUBLE,MPI_COMM_WORLD);
    }else{
        invOUT=invOUTLocal;
    }


    for(int global=globalStart;global<globalEnd;global++){
        analysisTemplate.timeMax=validDates->at(global);
        std::vector<analysisParameter> apsIN;
        generateBaseBounds(validDates->at(global),&analysisTemplate,sourceTickers,sourceDatapoints,sourceTickers,baseProbMargin,&apsIN);
        double inv = investment(MULTITHREAD_MODE,validDates->at(global),sourceTickers,sourceDatapoints,apsIN,analysis);
        invOUT.push_back(inv);
    }

    
}

void generateBaseBounds(long long testDate,analysisParameter *analysisTemplate,std::vector<ticker> *sourceTickers,std::vector<datapoint> *sourceDatapoints,std::vector<ticker> *testableTickers,int baseProbMargin,std::vector<analysisParameter> *results){
    for(ticker tt:*testableTickers){
        for(ticker st:*sourceTickers){
            if(st.id==tt.id){
                for(int i=tt.clusterStart;i<tt.clusterEnd+1;i++){
                    if(sourceDatapoints->at(i).time==testDate){
                        analysisParameter tempAP=*analysisTemplate;
                        tempAP.base=st;
                        if(tempAP.baseMax<tempAP.baseMin){
                            if(!tempAP.baseDayDiff){//single day change analysis
                                //Here we just need to limit prob distr by time limits
                                double sumOfChange=0;
                                int inTimeRange=0;
                                for(int dp=tempAP.base.clusterStart;dp<tempAP.base.clusterEnd+1;dp++){
                                    if(sourceDatapoints->at(dp).time<=tempAP.timeMax&&sourceDatapoints->at(dp).time>=tempAP.timeMin){
                                        sumOfChange += sourceDatapoints->at(dp).change;
                                        inTimeRange++;
                                    }
                                }
                                double mean = sumOfChange/(double)(inTimeRange);
                                double stdSum =0;
                                for(int dp=tempAP.base.clusterStart;dp<tempAP.base.clusterEnd+1;dp++){
                                    if(sourceDatapoints->at(dp).time<=tempAP.timeMax&&sourceDatapoints->at(dp).time>=tempAP.timeMin){
                                        double change = sourceDatapoints->at(dp).change;
                                        stdSum += (change-mean)*(change-mean);
                                    }
                                }
                                double std = sqrt(stdSum/(double)(inTimeRange));
                                double percentile = getPercentile((sourceDatapoints->at(i).change-mean)/std);
                                tempAP.baseMin= getZScore((percentile - baseProbMargin)<0?.01:(percentile - baseProbMargin))*std+mean;
                                tempAP.baseMax = getZScore((percentile + baseProbMargin)>1?.99:(percentile + baseProbMargin))*std+mean;
                                
                                
                            }else{//but in all other cases we must compute a new prob distr for multi days

                                //now for the hard part, we have to find the z score of the change value for our input 
                                //  dp but we do not know if it lies in the tickers dp vector, IE there may be no according multiday 
                                //  change if it is a standalone dp
                                //SO: we throw an exception if it doesnt lie in the vector, this is bc the program is not supr smart
                                // TODO: implement time based consecutivity checking for external data multiday base inc strategies
                                //THE common practice for multiday analysis it to then only use in vector data, and no external data
                                double testChange=0;
                                bool found=false;
                                for(int dp=tempAP.base.clusterStart+tempAP.baseDayDiff;dp<tempAP.base.clusterEnd+1;dp++){
                                    if(sourceDatapoints->at(dp).time==sourceDatapoints->at(i).time){ 
                                        testChange = sourceDatapoints->at(dp).adjClose/sourceDatapoints->at(dp-tempAP.baseDayDiff).adjClose-1;
                                        found==true;
                                    }
                                }
                                if(!found){
                                    throw("ERROR::a testing dp was not found in valid dp vector in basebounds generation in multi day");
                                    //if we dont find the testticker from testdate in sourcedp it is an error bc we have to have a multi day relation, which needs consecutive data in source
                                }

                                double sumOfChange=0;
                                int inTimeRange=0;
                                for(int dp=tempAP.base.clusterStart+tempAP.baseDayDiff;dp<tempAP.base.clusterEnd+1;dp++){
                                    if(sourceDatapoints->at(dp).time<=tempAP.timeMax&&sourceDatapoints->at(dp).time>=tempAP.timeMin&&
                                    sourceDatapoints->at(dp-tempAP.baseDayDiff).time<=tempAP.timeMax&&sourceDatapoints->at(dp-tempAP.baseDayDiff).time>=tempAP.timeMin
                                    ){
                                        sumOfChange += sourceDatapoints->at(dp).adjClose/sourceDatapoints->at(dp-tempAP.baseDayDiff).adjClose-1;
                                        inTimeRange++;
                                    }
                                }
                                double mean = sumOfChange/(double)(inTimeRange);
                                double stdSum =0;
                                for(int dp=tempAP.base.clusterStart+tempAP.baseDayDiff;dp<tempAP.base.clusterEnd+1;dp++){
                                    if(sourceDatapoints->at(dp).time<=tempAP.timeMax&&sourceDatapoints->at(dp).time>=tempAP.timeMin&&
                                    sourceDatapoints->at(dp-tempAP.baseDayDiff).time<=tempAP.timeMax&&sourceDatapoints->at(dp-tempAP.baseDayDiff).time>=tempAP.timeMin
                                    ){
                                        double change = sourceDatapoints->at(dp).adjClose/sourceDatapoints->at(dp-tempAP.baseDayDiff).adjClose-1;
                                        stdSum += (change-mean)*(change-mean);
                                    }
                                }
                                double std = sqrt(stdSum/(double)(inTimeRange));
                                double percentile = getPercentile((testChange-mean)/std);
                                tempAP.baseMin= getZScore((percentile - baseProbMargin)<0?.01:(percentile - baseProbMargin))*std+mean;
                                tempAP.baseMax = getZScore((percentile + baseProbMargin)>1?.99:(percentile + baseProbMargin))*std+mean;
                            }
                        }
                        results->push_back(tempAP);
                        //else do nothing, bounds should be set correctly
                    }
                }
            }
        }
    }
}

void standardAnalysis(int MULTITHREAD_MODE,std::vector<ticker> *sourceTickers,std::vector<datapoint> *sourceDatapoints,analysisParameter request,std::vector<analysisParameter> *results){
    std::vector<datapoint> bases;
    for(int i=request.base.clusterStart+request.baseDayDiff;i<request.base.clusterEnd+1;i++){
        double change = request.baseDayDiff==0?sourceDatapoints->at(i).change:sourceDatapoints->at(i).adjClose/sourceDatapoints->at(i-request.baseDayDiff).adjClose-1;
        if(change>=request.baseMin&&change<=request.baseMax&&
            sourceDatapoints->at(i).time>=request.timeMin&&sourceDatapoints->at(i).time<=request.timeMax
        ){
            bases.push_back(sourceDatapoints->at(i));
        }
    }
    for(ticker t:*sourceTickers){
        analysisParameter result=request;
        for(datapoint bDP:bases){
            for(int tDP =t.clusterStart;tDP<t.clusterEnd+1-request.targetDayOffset;tDP++){
                if(sourceDatapoints->at(tDP).time==bDP.time){
                    double change = request.targetDayDiff?sourceDatapoints->at(tDP+request.targetDayOffset).adjClose/sourceDatapoints->at(tDP+request.targetDayOffset-request.targetDayDiff).adjClose-1:(sourceDatapoints->at(tDP+request.targetDayOffset).change);
                    if(sourceDatapoints->at(tDP+request.targetDayOffset).time>request.timeMax) continue;
                    result.targetTotal++;
                    if(change>=request.targetMin&&change<=request.targetMax){
                        result.targetInBound++;
                    }
                }
            }
        }
        if(!result.targetTotal) continue;
        double zout = ((double)((double)result.targetInBound/(double)result.targetTotal)-result.pCutoff)/sqrt((1-result.pCutoff)*result.pCutoff/result.targetTotal);
        if(zout<result.zCutoff) continue;
        result.target = t;
        result.zCutoff =zout;
        results->push_back(result);
    }
}

double standardInvestment(int MULTITHREAD_MODE,long long testDate,std::vector<ticker> *sourceTickers,std::vector<datapoint> *sourceDatapoints,std::vector<analysisParameter> apsIN,void analysis(int,std::vector<ticker> *,std::vector<datapoint> *,analysisParameter,std::vector<analysisParameter> *)){
    
    int globalStart=0;
    int globalEnd=0;
    int localStart=0;
    int localEnd=0;
    int total = apsIN.size();
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        if(total<size){
            globalStart=0;
            globalEnd=total;
        }else{
            int partition =total/size;
            localStart=partition*rank;
            localEnd= partition*(rank+1);
            globalStart=partition*(size);
            globalEnd=total;
        }
    }else{
        globalStart=0;
        globalEnd=total;
        MULTITHREAD_MODE=MULTITHREAD_PARENT_ON;
    }

    std::vector<analysisParameter> apsOUTLocal;
    for(int local=localStart;local<localEnd;local++){
        analysis(MULTITHREAD_PARENT_ON,sourceTickers,sourceDatapoints,apsIN.at(local),&apsOUTLocal);
    }

    std::vector<analysisParameter> apsOUT;
    if(MULTITHREAD_MODE==MULTITHREAD_PARENT_OFF){
        int localapsOUTCount = apsOUTLocal.size();
        std::vector<int> localapsOUTCounts;
        localapsOUTCounts.resize(size);
        MPI_Allgather(&localapsOUTCount,1,MPI_INT,&localapsOUTCounts[0],1,MPI_INT,MPI_COMM_WORLD);
        std::vector<int> localapsOUTDispls;
        localapsOUTDispls.push_back(0);
        int totalapsOUT= localapsOUTCounts.at(0);
        for(int i=1;i<size;i++){
            totalapsOUT += localapsOUTCounts.at(i);
            localapsOUTDispls.push_back(localapsOUTDispls.at(i-1)+localapsOUTCounts.at(i-1));
        }
        apsOUT.resize(totalapsOUT);
        
        MPI_Allgatherv(&apsOUTLocal[0],localapsOUTCount,analysisParameterDatatype,(&apsOUT[0]),&localapsOUTCounts[0],&localapsOUTDispls[0],analysisParameterDatatype,MPI_COMM_WORLD);
    }else{
        apsOUT=apsOUTLocal;
    }

    for(int global=globalStart;global<globalEnd;global++){
        analysis(MULTITHREAD_MODE,sourceTickers,sourceDatapoints,apsIN.at(global),&apsOUT);
    }
    
    
    int right=0;
    int wrong=0;
    for(analysisParameter aps:apsOUT){
        for(ticker t:*sourceTickers){
            if(aps.target.id==t.id){
                for(int dp=t.clusterStart;dp<t.clusterEnd+1-aps.targetDayOffset;dp++){
                    if(sourceDatapoints->at(dp).time==testDate){
                        double change = aps.targetDayDiff?sourceDatapoints->at(dp+aps.targetDayOffset).adjClose/sourceDatapoints->at(dp+aps.targetDayOffset-aps.targetDayDiff).adjClose-1:sourceDatapoints->at(dp+aps.targetDayOffset).change;
                        if(change<=aps.targetMax&&change>=aps.targetMin){
                            right++;
                        }else{
                            wrong++;
                        }
                    }
                }
            }
        }
    }
    return right+wrong?0:right/(right+wrong);

}

#endif