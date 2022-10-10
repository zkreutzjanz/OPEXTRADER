
#include <stdio.h>
#include <math.h>  
#include <cmath>
#include <stddef.h>
#include <string.h>
#include <vector>
#include <time.h>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <sys/time.h>
#include <mpi.h>
#include "ostructs.hpp"
#include "oio.hpp"
#include "ologger.hpp"
#include "oanalysis.hpp"

unsigned long long convertNameToID(std::string in){
    int charsToPushFromName = in.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:in.size();
    char name[TICKER_NAME_LENGTH] = {' '};
    unsigned long long out=0;
    if(charsToPushFromName>0){
		for(int i=0;i<charsToPushFromName;i++){name[i] = in.at(i);}
        for(int i =0;i<TICKER_NAME_LENGTH;i++) {out |= (name[i] << (i*8));}
    }
    return out;
}

void declareDatatypes(){

	int lengths[] = {1,TICKER_NAME_LENGTH,1,1,1,1,1};
	MPI_Aint disps[] = {offsetof(datapoint,id),offsetof(datapoint,name),offsetof(datapoint,time),offsetof(datapoint,open),offsetof(datapoint,close),offsetof(datapoint,adjClose),offsetof(datapoint,change)};
	MPI_Datatype types[] = {MPI_LONG_LONG,MPI_CHAR,MPI_LONG_LONG,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(7,lengths,disps,types,&datapointDatatype);
	MPI_Type_commit(&datapointDatatype);

    int tlengths[] = {1,1,1,1,1};
	MPI_Aint tdisps[] = {offsetof(ticker,id),offsetof(ticker,clusterStart),offsetof(ticker,clusterEnd),offsetof(ticker,mean),offsetof(ticker,std)};
	MPI_Datatype ttypes[] = {MPI_LONG_LONG,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE};
	MPI_Type_create_struct(5,tlengths,tdisps,ttypes,&tickerDatatype);
	MPI_Type_commit(&tickerDatatype);


    int aplengths[] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
	MPI_Aint apdisps[] = {offsetof(analysisParameter,base),offsetof(analysisParameter,target),offsetof(analysisParameter,baseTotal),offsetof(analysisParameter,baseInBound),offsetof(analysisParameter,targetTotal),offsetof(analysisParameter,targetInBound),offsetof(analysisParameter,baseMin),offsetof(analysisParameter,baseMax),offsetof(analysisParameter,baseBoundMargin),offsetof(analysisParameter,targetMin),offsetof(analysisParameter,targetMax),offsetof(analysisParameter,targetZScore),offsetof(analysisParameter,targetPercentInc),offsetof(analysisParameter,baseDayDiff),offsetof(analysisParameter,targetDayDiff),offsetof(analysisParameter,timeMin),offsetof(analysisParameter,timeMax),offsetof(analysisParameter,targetDayOffset)};
	MPI_Datatype aptypes[] = {tickerDatatype,tickerDatatype,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG,MPI_LONG_LONG};
	MPI_Type_create_struct(18,aplengths,apdisps,aptypes,&analysisParameterDatatype);
	MPI_Type_commit(&analysisParameterDatatype);

}

void shareVectors(std::vector<datapoint> *datapoints,std::vector<ticker> *tickers,std::vector<long long> *days){
    int finalDatapointCount = datapoints->size();
    MPI_Bcast(&finalDatapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank>0) datapoints->resize(finalDatapointCount);
    MPI_Bcast(datapoints->data(),finalDatapointCount,datapointDatatype,0,MPI_COMM_WORLD);

    int finalTickerCount = tickers->size();
    MPI_Bcast(&finalTickerCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank>0) tickers->resize(finalTickerCount);
    MPI_Bcast(tickers->data(),finalTickerCount,tickerDatatype,0,MPI_COMM_WORLD);

    int finalDaysCount = days->size();
    MPI_Bcast(&finalDaysCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank>0) days->resize(finalDaysCount);
    MPI_Bcast(days->data(),finalDaysCount,MPI_LONG_LONG,0,MPI_COMM_WORLD);
}

void identifyTickersFromDatapoints(std::vector<datapoint> *in, std::vector<ticker> *out){
     #if LOG
	int tickersIdentified=0; 
	#endif

    for(int i =0;i<in->size();i++){
        bool found = false;
        int j=0;
        while(!found&&j<out->size()){
            if(in->at(i).id==out->at(j).id) found =true;
            j++;
        }
        if(!found){
            ticker newticker;
            newticker.id=in->at(i).id;
            out->push_back(newticker);
             #if LOG
		    tickersIdentified++;
		    if(tickersIdentified%100==0) Logger("Identified <"+std::to_string(out->size())+"> Tickers");
		    #endif
        }
       
    }
}

void generateTickerSchema(std::vector<datapoint> *in,std::vector<ticker> *out){
    ticker currentTicker;
    currentTicker.clusterStart=0;
    currentTicker.id=in->at(0).id;
    double sum=in->at(0).change;
    for(int i=1;i<in->size();i++){
        if(in->at(i).id!=currentTicker.id){
            currentTicker.clusterEnd=i-1;
            currentTicker.mean=(double)sum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1);
            
            double stdSum=0;
            for(int j = currentTicker.clusterStart;j<currentTicker.clusterEnd+1;j++){
                stdSum += (in->at(j).change-currentTicker.mean)*(in->at(j).change-currentTicker.mean);
               // log("INNER LOOP:::"+std::to_string((in->at(j).change-currentTicker.mean)*(in->at(j).change-currentTicker.mean))+":::"+std::to_string(in->at(j).change)+"::"+std::to_string(currentTicker.mean));
            
            }
            currentTicker.std= sqrt(stdSum/(double)(currentTicker.clusterEnd-currentTicker.clusterStart+1));
            Logger(std::to_string(i)+":::"+std::to_string(sum)+"::"+std::to_string(stdSum)+"::"+std::to_string(currentTicker.clusterEnd-currentTicker.clusterStart+1)+"::"+std::to_string(currentTicker.mean)+"::"+std::to_string(currentTicker.std));
            out->push_back(currentTicker);
            currentTicker.id=in->at(i).id;
            currentTicker.clusterStart=i;
            currentTicker.clusterEnd=0;
            currentTicker.mean=0;
            currentTicker.std=0;
            sum=in->at(i).change;
        }
        sum += in->at(i).change;
    }
}

void generateTradableDaysVector(std::vector<datapoint> *datapointVector,std::vector<ticker> *tickerVector,std::vector<long long> *daysVector){
    if(!datapointVector->size()) return;
    long long first=datapointVector->at(0).time;
    long long last=first;
    daysVector->push_back(last);
    for(ticker t:*tickerVector){
        for(int d=t.clusterStart;d<t.clusterEnd+1;d++){
            if(datapointVector->at(d).time>last){
                last=datapointVector->at(d).time;
                daysVector->push_back(last);
            }else if(datapointVector->at(d).time<first){
                first = datapointVector->at(d).time;
                daysVector->insert(daysVector->begin(),first);
            }
        }
    }
}

void writeVectorsToFiles(std::string datapointFileName,std::string tickerFileName,std::vector<datapoint> *datapointVector,std::vector<ticker> *tickerVector){
    AddHeader("loadingDatapointsVectorToFile::");
    loadDatastructsToFile(&datapointFileName,datapointVector,&parseDataPointToStorageLine);
    RemoveHeader();

    AddHeader("loadingTickerSchemasVectorToFile::");
    loadDatastructsToFile(&tickerFileName,tickerVector,&parseTickerToStorageLine);
    RemoveHeader();
}

void initialization(std::string inputFileName,std::vector<datapoint> *targetDatapointVector,std::vector<ticker> *targetTickerVector,std::vector<long long> *targetDaysVector){

    //O(1)
    AddHeader("declareDatatypes::");
    declareDatatypes();
    RemoveHeader("initialization::Declared Datatypes");

    //Unknown BC IO
    AddHeader("loadFileToDatapointsVector::");
    std::vector<datapoint> unClusteredDatapoints;
    if(rank ==0) loadFileToDataStructure(&inputFileName,&unClusteredDatapoints,&parseSourceCSVLineToDataPoint);
    RemoveHeader("initialization::Loaded File to Datapoints Vector");

    //Unknown BC MPI
    AddHeader("sharingUnClusteredDatapoints::");
    int datapointCount = unClusteredDatapoints.size();
    MPI_Bcast(&datapointCount,1,MPI_INT,0,MPI_COMM_WORLD);
    if(rank!=0) unClusteredDatapoints.resize(datapointCount);
    MPI_Bcast(&unClusteredDatapoints[0],datapointCount,datapointDatatype,0,MPI_COMM_WORLD);
    RemoveHeader("initialization::Shared UnClustered Datapoints");

    //~O(datapointcount/cores)
    AddHeader("getLocalUnClusteredDatapoints::");
    std::vector<datapoint> localUnClusteredDatapoints;
    for(int i = rank;i<datapointCount;i+=size){
        localUnClusteredDatapoints.push_back(unClusteredDatapoints.at(i));
    }
    RemoveHeader("initialization::Got Local UnClustered Datapoints");

    //~O(datapointcount*tickercount/cores)
    AddHeader("identifyLocalUnClusteredTickers::");
    std::vector<ticker> localUnClusteredTickers;
    identifyTickersFromDatapoints(&localUnClusteredDatapoints,&localUnClusteredTickers);
    RemoveHeader("initialization::Identified Local UnClustered Tickers");

    //Unknown BC MPI
    AddHeader("regatherUnClusteredTickers::");
    int localUnClusteredTickersCount = localUnClusteredTickers.size();
    std::vector<ticker> tickers;
    std::vector<int> unClusteredTickersCounts;
    unClusteredTickersCounts.resize(size);
    std::vector<int> unClusteredTickersDispls;
    unClusteredTickersDispls.push_back(0);
    MPI_Allgather(&localUnClusteredTickersCount,1,MPI_INT,&unClusteredTickersCounts[0],1,MPI_INT,MPI_COMM_WORLD);
    int tickerCountsSum = unClusteredTickersCounts.at(0);
    for(int i=1;i<size;i++){
        unClusteredTickersDispls.push_back(unClusteredTickersDispls.at(i-1)+unClusteredTickersCounts.at(i-1));
        tickerCountsSum += unClusteredTickersCounts.at(i);
    }
    Logger(std::to_string(tickerCountsSum));
    tickers.resize(tickerCountsSum);
    MPI_Allgatherv(&localUnClusteredTickers[0],localUnClusteredTickersCount,tickerDatatype,&tickers[0],&unClusteredTickersCounts[0],&unClusteredTickersDispls[0],tickerDatatype,MPI_COMM_WORLD);
    RemoveHeader("initialization::Regathered UnClustered Tickers");

    //Unknown BC std::
    AddHeader("deDuplicateUnClusteredTickers::");
    std::sort(
        tickers.begin(),
        tickers.end(),
        [](ticker &a,ticker &b){
            return a.id<b.id;
        }
    );
    auto last = std::unique(
        tickers.begin(), 
        tickers.end(),
        [](ticker &a,ticker &b){
            return a.id==b.id;
        }
    );
    tickers.erase(last, tickers.end());
    RemoveHeader();


    //~O(tickercount/cores)
    AddHeader("identifyLocalTickers::");
    std::vector<ticker> localTickers;
    int commonPartition = (tickers.size()/size>0)?tickers.size()/size:1;
    for(int i =0;i<tickers.size();i++){
        if(rank==i%size) localTickers.push_back(tickers.at(i));
    }
    RemoveHeader();


    //The next two sections could be combined
    //IE identify and sort on one run, 
    //thing is I dont want to implemnt my own sorting alogoritm

    //~O(datapointcount*tickercount/cores)
    AddHeader("iidentifyLocalDatapoints::");
    std::vector<datapoint> localDatapoints;
    for(int i=0;i<unClusteredDatapoints.size();i++){
        for(int j = 0;j<localTickers.size();j++){
            if(localTickers.at(j).id==unClusteredDatapoints.at(i).id){
                localDatapoints.push_back(unClusteredDatapoints.at(i));
                j=localTickers.size();
            }
        }
    }
    std::vector<ticker>().swap(localTickers);
    std::vector<ticker>().swap(tickers);
    RemoveHeader();


    //~O(?) sorting
    AddHeader("sortDatapoints::");
    std::sort(
        localDatapoints.begin(),
        localDatapoints.end(),
        [](datapoint &a,datapoint &b){
            if(a.id<b.id) return true;
            if(a.id>b.id) return false;
            return a.time<b.time;
        }
    );
    RemoveHeader();


    //regather O(unkown)
    AddHeader("regatherDatapoints::");
    int localDatapointCount = localDatapoints.size();
    if(rank==0) targetDatapointVector->resize(datapointCount);
    std::vector<int> datapointCounts;
    datapointCounts.resize(size);
    std::vector<int> datapointDispls;
    if(rank==0) datapointDispls.push_back(0);
    MPI_Gather(&localDatapointCount,1,MPI_INT,&datapointCounts[0],1,MPI_INT,0,MPI_COMM_WORLD);
    for(int i=1;i<size;i++){
        if(rank==0) datapointDispls.push_back(datapointDispls.at(i-1)+datapointCounts.at(i-1));
    }
    MPI_Gatherv(&localDatapoints[0],localDatapointCount,datapointDatatype,targetDatapointVector->data(),&datapointCounts[0],&datapointDispls[0],datapointDatatype,0,MPI_COMM_WORLD);
    std::vector<datapoint>().swap(localDatapoints);
    std::vector<datapoint>().swap(unClusteredDatapoints);
    RemoveHeader("");



    AddHeader("generateTickerSchema::");
    if(rank==0) generateTickerSchema(targetDatapointVector,targetTickerVector);
    RemoveHeader();

    AddHeader("Generate tradable days Vector::");
    generateTradableDaysVector(targetDatapointVector,targetTickerVector,targetDaysVector);
    RemoveHeader();
    
    AddHeader("Share Vectors::");
    shareVectors(targetDatapointVector,targetTickerVector,targetDaysVector);
    RemoveHeader();

}

void startup(std::string datapointFileName,std::string tickerSchemaFileName,std::vector<datapoint> *targetDatapointVector,std::vector<ticker> *targetTickerVector,std::vector<long long> *targetDaysVector){
    Logger("startup::Declaring Datatypes");
    declareDatatypes();
    Logger("startup::Declared Datatypes");

    Logger("startup::Loading File to Datapoints Vector");
    if(rank==0) loadFileToDataStructure(&datapointFileName,targetDatapointVector,&parseStorageLineToDataPoint);
    Logger("startup::Loaded File to Datapoints Vector");

    Logger("startup::Loading File ticker schemas Vector");
    if(rank==0) loadFileToDataStructure(&tickerSchemaFileName,targetTickerVector,&parseStorageLineToTicker);
    Logger("startup::Loaded File to ticker schemas Vector");

    Logger("startup::Generating tradable days Vector");
    generateTradableDaysVector(targetDatapointVector,targetTickerVector,targetDaysVector);
    Logger("startup::Generated tradable days Vector");

    Logger("startup::sharing vectors");
    shareVectors(targetDatapointVector,targetTickerVector,targetDaysVector);
    Logger("startup::shared vectors");

}

/*
void pruneGlobalVectorsToGivenLength(int lines){

    Logger("pruneGlobalVectorsToGivenLength::Resizing Datapoints Vector");
    globalDatapoints.resize(lines);
    Logger("pruneGlobalVectorsToGivenLength::Resized Datapoints Vector");

    Logger("pruneGlobalVectorsToGivenLength::Recreating Ticker Schema");
    std::vector<ticker> newTickers;
    for(int i=0;i<globalTickers.size();i++){
        if(globalTickers.at(i).clusterEnd>=lines){
            globalTickers.at(i).clusterEnd=lines-1;
            double meanSum=0;
            for(int j = globalTickers.at(i).clusterStart;j<globalTickers.at(i).clusterEnd;j++) meanSum += globalDatapoints.at(j).change;
            globalTickers.at(i).mean = meanSum/globalTickers.at(i).clusterEnd-globalTickers.at(i).clusterStart;
            double stdSum=0;
            for(int j = globalTickers.at(i).clusterStart;j<globalTickers.at(i).clusterEnd;j++){
                stdSum += (globalDatapoints.at(j).change-globalTickers.at(i).mean)*(globalDatapoints.at(j).change-globalTickers.at(i).mean);
            
            }
            globalTickers.at(i).std= sqrt(stdSum/(double)(lines-globalTickers.at(i).clusterStart));

            break;
        }else{
            newTickers.push_back(globalTickers.at(i));
        }
    }
    globalTickers.swap(newTickers);
    Logger("pruneGlobalVectorsToGivenLength::Recreated Ticker Schema");

}*/

/*
void filterGlobalVectorsByGivenTickers(std::vector<std::string> filterTickers){

    AddHeader("populateNewDatapointsAndTickerVectors::");
    std::vector<datapoint> newDatapoints;
    std::vector<ticker> newTickers;
    for(std::string currentFilterTicker:filterTickers){
        int myID=0;
        int charsToPushFromTickerName = currentFilterTicker.size()>TICKER_NAME_LENGTH?TICKER_NAME_LENGTH:currentFilterTicker.size();
        if(charsToPushFromTickerName){
            char name[TICKER_NAME_LENGTH]={' '};
            for(int i=0;i<charsToPushFromTickerName;i++) name[i] = currentFilterTicker.at(i);
            for(int i =0;i<TICKER_NAME_LENGTH;i++) myID |= (name[i] << (i*8));
        }
        if(myID){
            for(ticker currentTicker:globalTickers){
                if(currentTicker.id==myID){
                    ticker newTicker = currentTicker;
                    newTicker.clusterStart=newDatapoints.size();
                    for(int i = currentTicker.clusterStart;i<currentTicker.clusterEnd+1;i++){
                        newDatapoints.push_back(globalDatapoints.at(i));
                    }
                    newTicker.clusterEnd=newDatapoints.size()-1;
                    newTickers.push_back(newTicker);
                    break;
                }
            }
        }
    }
    RemoveHeader();

    AddHeader("setGlobalVectorsToNewVectors::");
    globalDatapoints.swap(newDatapoints);
    globalTickers.swap(newTickers);
    RemoveHeader();

}*/


int main(int argc,char** argv){

    MPI_Init(&argc,&argv);

    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&size);


    /**
    AddHeader("initialization::");
    initialization("../assets/historical_daily_data_kaggle");
    RemoveHeader();

    AddHeader("writeInitVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    RemoveHeader();

     AddHeader("filterInitVectors::");
    std::vector<std::string> in = {"KR","PG","AAPL","KO","MSFT"};
    filterGlobalVectorsByGivenTickers(in);
    RemoveHeader();

    AddHeader("writeFilteredVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    RemoveHeader();
**/

/*
    //run this when you get home
    AddHeader("startup::");
    startup("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    
    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    RemoveHeader();

    /**
    AddHeader("filterInitVectors::");
    std::vector<std::string> in = {"AAPL","AMZN","FB","TSLA","F","T","VZ","F","GPRO","KO","MSFT"};
    filterGlobalVectorsByGivenTickers(in);
    RemoveHeader();

    AddHeader("writeFilteredVectorsToFiles::");
    writeGlobalVectorsToFiles("../assets/KRGEdeepstorage","../assets/KRGESchemadeepstorage");
    RemoveHeader();
    **/

   /**

    MPI_Barrier(MPI_COMM_WORLD);
    Logger("to dps");
    MPI_Barrier(MPI_COMM_WORLD);

    std::vector<datapoint> testDps;
    datapoint dp1=datapoint{
            .id=convertNameToID("AAPL"),
            .name="AAPL",
            .open=146.28,
            .close=149.84,
            .adjClose=149.84,
            .change=-.0129
        };
    datapoint dp2 = datapoint{
            .id=convertNameToID("AMZN"),
            .name="AMZN",
            .time=0,
            .open=114.03,
            .close=118.05,
            .adjClose=118.05,
            .change=.0312
    };
    testDps.push_back(dp1);
    testDps.push_back(dp2);
    
    MPI_Barrier(MPI_COMM_WORLD);
    Logger("dps created");
    MPI_Barrier(MPI_COMM_WORLD);
    analysisParameter testAP = analysisParameter
    {
    .baseMin=__DBL_MAX__,
    .baseMax=__DBL_MIN__,
    .baseBoundMargin=.05,
    .targetMin=-.01,
    .targetMax=.01,
    .targetZScore=0,
    .targetPercentInc=.5,
    .baseDayDiff=0,
    .targetDayDiff=1,
    .timeMin=LONG_LONG_MIN,
    .timeMax=LONG_LONG_MAX,
    .targetDayOffset=1
    };

    //runAnalysisOnDay(MULTITHREAD_PARENT_ON,thisAP,&rightness,&prof,aps,&results,&safeAnalysis);
   // Logger(rightness);
    //Logger(prof);

    /**}
    catch (const char* msg) {
     std::cerr << msg << std::endl;
   }**/

   
    //double normalProfit = backtestDailyInvestmentStrategy(MULTITHREAD_PARENT_OFF,strat,&garbage,globalDatapoints.at(globalDatapoints.size()-150).time,globalDatapoints.at(globalDatapoints.size()-100).time,0,&longTermInvestmentStrategy);
    

   

    
    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    
/**
        double out = (backtestDailyInvestmentStrategy(strat,&results,clusteredDatapoints.at(clusteredDatapoints.size()-2000).time,clusteredDatapoints.at(clusteredDatapoints.size()-1).time,1.64,&dayAfterIncStrategy));
        std::cout<<std::to_string(out)<<"\n";
        if(rank==0){
            std::string toshow = std::to_string(i)+";"+std::to_string(clusteredDatapoints.at(clusteredDatapoints.size()-i).time)+";"+std::to_string(out);
             std::cout<<toshow<<"\n";
             results.push_back(toshow);
        }
    }

    std::string fileName= "../assets/results";
    loadDatastructsToFile(&fileName,&results,&parseStringToStorageLine);**/
    
   

    //startup("../assets/datapointdeepstorage","../assets/tickerschemadeepstorage");
    
    

    MPI_Finalize();

}
////////////////////////////////////////////////////////////////////////////////////////////////////