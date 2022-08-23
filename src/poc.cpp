#include <mpi.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <vector>
#include <time.h>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <math.h>

#define OFF 0
#define ON 1
#define BENCHMARK ON
#define LOG ON

const int TICKER_NAME_LENGTH = 7;

/**
 * @brief For implicit compile time filtering
 * 
 * @param message 
 */
void log(std::string message){
	#if LOG
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	std::string output= "NODE: "+std::to_string(rank)+" LOG: "+message+"\n";
	std::cout<<output;
	#endif
}

/**
 * @brief For explicit in code compile time filtering
 * 
 * @param message 
 */
void benchmark(std::string message){
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	std::string output= "NODE: "+std::to_string(rank)+" BENCHMARK: "+message+"\n";
	std::cout<<output;
}




/**
 * @brief Stores data from a time with open and close for a ticker
 * 
 */
struct dataPoint{
	char name[TICKER_NAME_LENGTH]="";
	double open;
	double adjClose;
	double percentChange;
	long long time;
};
/**
 * @brief Stores Vector of datapoints for given ticker
 * 
 */
struct ticker{
	char name[TICKER_NAME_LENGTH]="";
	std::vector<dataPoint> dataPoints;
};

/**
 * @brief parses line into datapoint struct
 * 
 * @param out 
 * @param line 
 * @param delimiter 
 * @return true: line was parsed to datapoint
 * @return false: line could not be parsed to datapoint
 */
bool parseLineInputToDataPointFX(dataPoint *out,std::string line,std::string delimiter){
	std::string name;
	struct std::tm tm ={0};
	double open=0;
	double close=0;
	double adjClose=0;
	long long time;
	try{
	name = line.substr(0,line.find(delimiter));line.erase(0,line.find(delimiter)+1);//name
  	line.erase(0,line.find(delimiter)+1);//something
        std::istringstream ss(line.substr(0,line.find(delimiter)));
        ss >> std::get_time(&tm, "%Y-%m-%d"); 
    time= mktime(&tm);line.erase(0,line.find(delimiter)+1);//time
	open = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//open
    line.erase(0,line.find(delimiter)+1);//high
	line.erase(0,line.find(delimiter)+1);//low
    close = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//close
	adjClose = stod(line.substr(0,line.find(delimiter)));line.erase(0,line.find(delimiter)+1);//adjclose
    line.erase(0,line.find(delimiter)+1);//dividend
	line.erase(0,line.find(delimiter)+1);//volume
	}catch(std::exception e){}
	
	int charsToPushFromName = name.size()>7?7:name.size();
	
    if(open&&charsToPushFromName>0){
		
		if(adjClose){
			
			for(int i=0;i<charsToPushFromName;i++) 
				out->name[i] = name.at(i);
			
			out->adjClose = adjClose;
			out->time=time;
			out->open = open;
			out->percentChange = (adjClose/open)-1;
			return true;
		}else if(close){
			for(int i=0;i<charsToPushFromName;i++)
				out->name[i] = name.at(i);
			out->adjClose = close;
			out->open = open;
			out->percentChange = (close/open)-1;
			return true;
		}else return false;
	}else return false;
       
}

/**
 * @brief Converts specified file data to datapoints vector
 * 
 * @param sourceFileName 
 * @param dataPointsClsuter 
 */
void buildDataPointsComponent(std::string *sourceFileName,std::vector<dataPoint> *dataPointsClsuter){
	std::ifstream myFile;
	log("Opening file <"+*sourceFileName+">");
	myFile.open(*sourceFileName);
	log("File <"+*sourceFileName+"> opened");
	std::string line;
    getline(myFile, line);
	#if BENCHMARK
	int linesParsed=0; 
	#endif
	while(getline(myFile, line))
    {
        dataPoint temp;
        bool readable = parseLineInputToDataPointFX(&temp,line,";");
        if(readable) dataPointsClsuter->push_back(temp);
		
		#if BENCHMARK
		linesParsed++;
		if(linesParsed%100000==0) benchmark(std::to_string(linesParsed)+" lines parsed");
		#endif
    }
};

/**
 * @brief Nonstandardized datapoint order, nonthreaded, groups datapoints into tickers
 * [Complexity: ~O(n(t+1)/2) ∋ n=datapoints ∩ t = tickers ∩ ∀t.size()=]
 */
void nonstandardizedTickerGrouping(std::vector<dataPoint> *input,std::vector<ticker> *output){
	#if BENCHMARK
	int total = input->size();
	int complete = 0;
	double jump = .001;
	int iteration = jump*total;
	#endif
	for(dataPoint dp : *input){
		log("On datapoint:"+(std::string)dp.name);
		bool found = false;
		for(int i=0;i<output->size();i++){
			if(strcmp(output->at(i).name,dp.name)==0){
				output->at(i).dataPoints.push_back(dp);
				found=true;
			}
		}
		if(!found){
			ticker newTicker;
			memcpy(newTicker.name,dp.name,TICKER_NAME_LENGTH);
			newTicker.dataPoints.push_back(dp);
			output->push_back(newTicker);
		}
		#if BENCHMARK
		complete++;
		if(complete%iteration==0)
			benchmark("Non Standardized Ticker Grouping "+std::to_string((double)100*complete/total)+"% Complete");

		#endif
	}
}
/**
 * @brief standardized datapoint order, nonthreaded, groups datapoints into tickers
 * [Complexity: ~O(n) ∋ n=datapoints]
 */
void standardizedTickerGrouping(std::vector<dataPoint> *input,std::vector<ticker> *output){
	#if BENCHMARK
	int total = input->size();
	int complete = 0;
	double jump = .001;
	int iteration = jump*total;
	#endif
	ticker currentTicker;
	dataPoint previousDatapoint;
	for(dataPoint dp : *input){
		if(strcmp(previousDatapoint.name,dp.name)==0){
			currentTicker.dataPoints.push_back(dp);
		}else{
			if(strcmp(currentTicker.name,"")!=0){
				output->push_back(currentTicker);
			}
			memcpy(currentTicker.name,dp.name,TICKER_NAME_LENGTH);
			currentTicker.dataPoints.clear();
			currentTicker.dataPoints.push_back(dp);
		}
		#if BENCHMARK
		complete++;
		if(complete%iteration==0)
			benchmark("Standardized Ticker Grouping "+std::to_string((double)100*complete/total)+"% Complete");

		#endif
	}
	output->push_back(currentTicker);
}
/**
 * @brief 
 * ~~~~Inefficient, uncoded~~~~
 * Nonstandardized datapoint order, multithreaded, groups datapoints into tickers
 * [Complexity: ~O(n(t+1)/2c)+~O(t(t+1)/2) ∋ n=datapoints ∩ t = tickers ∩ c = threads ∩ ∀t.size()=]
 */
void nonstandardizedThreadedTickerGrouping(){}
/**
 * @brief standardized datapoint order, multithreaded, groups datapoints into tickers
 * [Complexity: ~O(n/c)+~O(c) ∋ n=datapoints ∩ t = tickers ∩ c = threads ∩ ∀t.size()=]
 */
void standardizedThreadedTickerGroupingComponent(std::vector<dataPoint> *input,std::vector<ticker> *output){
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	

	///create the datapoint datatype to be transferred between nodes
	MPI_Datatype dataPointDatatype;
	int lengths[] = {TICKER_NAME_LENGTH,1,1,1,1};
	MPI_Aint types[] = {offsetof(dataPoint,name),offsetof(dataPoint,open),offsetof(dataPoint,adjClose),offsetof(dataPoint,percentChange),offsetof(dataPoint,time)};
	int disps[] = {MPI_CHAR,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG};
	MPI_Type_create_struct(5,lengths,types,disps,&dataPointDatatype);
	MPI_Type_commit(&dataPointDatatype);
	///broadcast the size of the datapoint vector to all nodes
	int inputSize=0;
	if(rank==0) inputSize = input->size();
	MPI_Bcast(&inputSize,1,MPI_INT,0,MPI_COMM_WORLD);

	///calculate the size and displacement of each partition per node
	int partitionSize = inputSize/(size-1);
	int* recvCounts = new int[size];
	int* sendDisp = new int[size];
	recvCounts[0] = 0;
	sendDisp[0]=0;
	for(int i =1;i<size-1;i++){
		recvCounts[i]=partitionSize;
		sendDisp[i] = partitionSize*(i-1);
		
	} 
	recvCounts[size-1] = inputSize-(size-2)*partitionSize;
	sendDisp[size-1]=(size-2)*partitionSize;
	std::vector<dataPoint> myDatapoints(recvCounts[rank]);
	log("Partition Size: "+std::to_string(partitionSize)+"recv:"+std::to_string(recvCounts[rank])+"disp:"+std::to_string(sendDisp[rank]));
	///scatter datapoint vector
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Scatterv(&(*input)[0], recvCounts, sendDisp, dataPointDatatype,
    &myDatapoints[0], recvCounts[rank], dataPointDatatype,
    0, MPI_COMM_WORLD);
	
	MPI_Barrier(MPI_COMM_WORLD);

	///if root, recieve, else, group
	if(rank==0){
		int finishedNodeCounter=0;
		while(1){
			MPI_Status currentStatus;
			int flag=0;
			MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&flag,&currentStatus);
			if(flag){
				int recvLength;
				MPI_Get_count(&currentStatus,dataPointDatatype,&recvLength);
				std::vector<dataPoint> recvdp(recvLength);
				MPI_Recv(&recvdp[0],recvLength,dataPointDatatype,MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&currentStatus);
				log("Grouping ACK: "+(std::string)recvdp.at(0).name);
				bool found = false;
				for(int i=0;i<output->size();i++){
					if(strcmp(output->at(i).name,recvdp.at(0).name)==0){
						for(dataPoint dp : recvdp) output->at(i).dataPoints.push_back(dp);
						found=true;
					}
				}
				if(!found){
					ticker newTicker;
					memcpy(newTicker.name,recvdp.at(0).name,TICKER_NAME_LENGTH);
					for(dataPoint dp : recvdp) newTicker.dataPoints.push_back(dp);
					output->push_back(newTicker);
				}
				if(!currentStatus.MPI_TAG){
					finishedNodeCounter++;

					if(finishedNodeCounter==size-1){
						break;
					}
				}
			}
		}	
	}else{
		
		dataPoint previousDatapoint;
		std::vector<MPI_Request> myRequests;
		std::vector<ticker> myTickers;
		#if BENCHMARK
		int total = myDatapoints.size();
		int complete = 0;
		double jump = .1;
		int iteration = jump*total;
		#endif
		for(dataPoint dp : myDatapoints){
			if(strcmp(previousDatapoint.name,dp.name)==0){
				myTickers.at(myTickers.size()-1).dataPoints.push_back(dp);
			}else{
				if(myTickers.size()>0){
					
					MPI_Request myRequest;
					myRequests.push_back(myRequest);
					//MPI_Ibcast(&(myTickers.at(myTickers.size()-1).dataPoints[0]),myTickers.at(myTickers.size()-1).dataPoints.size(),dataPointDatatype,rank,MPI_COMM_WORLD,&(myRequests.at(myRequests.size()-1)));
					MPI_Isend(&(myTickers.at(myTickers.size()-1).dataPoints[0]),myTickers.at(myTickers.size()-1).dataPoints.size(),dataPointDatatype,0,1,MPI_COMM_WORLD,&(myRequests.at(myRequests.size()-1)));
					log("Grouping SYN: "+(std::string)myTickers.at(myTickers.size()-1).name);
					
				}
				ticker myTicker;
				
				memcpy(myTicker.name,dp.name,TICKER_NAME_LENGTH);
				myTicker.dataPoints.push_back(dp);
				myTickers.push_back(myTicker);
			}
			previousDatapoint=dp;
			#if BENCHMARK
			complete++;
			if(complete%iteration==0)
				benchmark("Grouping "+std::to_string((double)100*complete/total)+"% Complete");

			#endif
		}
		
		MPI_Request myRequest;
		myRequests.push_back(myRequest);
		MPI_Isend(&(myTickers.at(myTickers.size()-1).dataPoints[0]),myTickers.at(myTickers.size()-1).dataPoints.size(),dataPointDatatype,0,0,MPI_COMM_WORLD,&(myRequests.at(myRequests.size()-1)));
		std::vector<MPI_Status> statuses(myRequests.size());
		MPI_Waitall(myRequests.size(),&(myRequests[0]),&(statuses[0]));
	}

}

void twoTickerIncreaseAnalysisComponent(std::vector<ticker> *input){
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	double fDPMIN = .00;
	double fDPMAX = 1;
	double sDPMIN = .001;
	double sDPMAX = 1;
	double likelihood = .5;

	//create the datapoint datatype to be transferred between nodes
	MPI_Datatype dataPointDatatype;
	int lengths[] = {TICKER_NAME_LENGTH,1,1,1,1};
	MPI_Aint types[] = {offsetof(dataPoint,name),offsetof(dataPoint,open),offsetof(dataPoint,adjClose),offsetof(dataPoint,percentChange),offsetof(dataPoint,time)};
	int disps[] = {MPI_CHAR,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG};
	MPI_Type_create_struct(5,lengths,types,disps,&dataPointDatatype);
	MPI_Type_commit(&dataPointDatatype);

	

	//broadcast all data/syncs up input ticker vector from master
	if(rank==0){
		
		#if BENCHMARK
		int total = input->size();
		int complete = 0;
		double jump = .1;
		int iteration = jump*total;
		#endif
		for(int i=0;i<input->size()-1;i++){
			std::vector<MPI_Request> myRequests;
			
			for(int j=1;j<size;j++){
				MPI_Request myRequest;
				myRequests.push_back(myRequest);
				int id = myRequests.size()-1;
				
				//log("My requests size"+std::to_string(myRequests.size())+"input size"+std::to_string(input->at(i).dataPoints.size()));
				MPI_Isend(&(input->at(i).dataPoints[0]),input->at(i).dataPoints.size(),dataPointDatatype,j,1,MPI_COMM_WORLD,&myRequests.at(id));
				
				//log("sent"+std::to_string(i)+"of"+std::to_string(input->size())+"on"+std::to_string(j));
			}
			log("ALL "+std::to_string(input->size())+" Spread SYN "+(std::string)input->at(i).dataPoints[0].name);
			std::vector<MPI_Status> statuses(myRequests.size());
			
			MPI_Waitall(myRequests.size(),&(myRequests[0]),&(statuses[0]));
			
			myRequests.clear();
			#if BENCHMARK
			complete++;
			if(complete%iteration==0)
				benchmark("Spread "+std::to_string((double)100*complete/total)+"% Complete");

			#endif
		}
		std::vector<MPI_Request> myRequests;
		for(int j=1;j<size;j++){
			MPI_Request myRequest;
			myRequests.push_back(myRequest);
			MPI_Isend(&(input->at(input->size()-1).dataPoints[0]),input->at(input->size()-1).dataPoints.size(),dataPointDatatype,j,0,MPI_COMM_WORLD,&(myRequests.at(myRequests.size()-1)));
			

		}
		log("Spread SYN "+(std::string)input->at(input->size()-1).dataPoints[0].name);
		std::vector<MPI_Status> statuses(myRequests.size());
		MPI_Waitall(myRequests.size(),&(myRequests[0]),&(statuses[0]));

	}else{
		#if BENCHMARK
		int linesParsed=0; 
		#endif
		while(1){
			MPI_Status currentStatus;
			int flag=0;
			
			MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&flag,&currentStatus);
			if(flag){
				
				int recvLength;
				MPI_Get_count(&currentStatus,dataPointDatatype,&recvLength);
				std::vector<dataPoint> recvdp(recvLength);
				MPI_Recv(&recvdp[0],recvLength,dataPointDatatype,MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&currentStatus);
				log("ALL "+std::to_string(input->size())+" Spread ACK "+(std::string)recvdp.at(0).name);
				bool found = false;
				for(int i=0;i<input->size();i++){
					if(strcmp(input->at(i).name,recvdp.at(0).name)==0){
						for(dataPoint dp : recvdp) input->at(i).dataPoints.push_back(dp);
						found=true;
					}
				}
				if(!found){
					ticker newTicker;
					memcpy(newTicker.name,recvdp.at(0).name,TICKER_NAME_LENGTH);
					for(dataPoint dp : recvdp) newTicker.dataPoints.push_back(dp);
					input->push_back(newTicker);
				}
				#if BENCHMARK
				linesParsed++;
				if(linesParsed%10==0) benchmark(std::to_string(linesParsed)+" Spread SYNS");
				#endif
				if(!currentStatus.MPI_TAG){

					break;
				}
			}
		}	
	}
	
	MPI_Barrier(MPI_COMM_WORLD);

	//what tickers do i run this node on??
	int inputsize = input->size();
	int leftovers = inputsize%(size);
	int partition = rank==(size-1)?(inputsize/size)+leftovers:inputsize/size;
	int start = rank*partition;
	int end = start+partition;
	log(" Input Size: "+std::to_string(inputsize)+
		" My Partition: "+std::to_string(partition)+
		" My Start: "+std::to_string(start)+
		" My End: "+std::to_string(end));

	//now analyze
	for(int i = start;i<end;i++){
		for(ticker secondaryTicker:*input){
			//log("Ticker Pair: "+(std::string)input->at(i).name+", "+(std::string)secondaryTicker.name);
			int lookedAt=0;
			int insideBounds =0;
			for(dataPoint firstDP:input->at(i).dataPoints){
				if(firstDP.percentChange>=fDPMIN&&firstDP.percentChange<=fDPMAX){
					long long dayAfter  = (firstDP.time-(9/24)*86400)%(7*86400)==0?(long long)firstDP.time+86400*3:(long long)firstDP.time+86400;
					for(dataPoint secondDP:secondaryTicker.dataPoints){
						if(secondDP.time==dayAfter){
							lookedAt++;
							if(secondDP.percentChange>=sDPMIN&&secondDP.percentChange<=sDPMAX){
								insideBounds++;
							}
						}
					}
				}
			}
			//log("Ticker Pair: "+(std::string)input->at(i).name+", "+(std::string)secondaryTicker.name+" InsideBounds/LookedAt: "+std::to_string(insideBounds)+"/"+std::to_string(lookedAt));
			if(lookedAt&&insideBounds){
				double z = ((double)(insideBounds/lookedAt)-likelihood)/sqrt((likelihood*(1-likelihood))/(double)lookedAt);
				if(z>1) log("Z-Score "+std::to_string(z)+" found in ticker "+(std::string)input->at(i).name+" at ticker "+(std::string)secondaryTicker.name+" looked at "+std::to_string(lookedAt)+" and found "+std::to_string(insideBounds));
				
			}
		}
	}
	
}



int main(int argc,char** argv){
	MPI_Init(NULL,NULL);
	
	int size;
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);

	//Seup the MPI datatype for the custom datapoint struct
	MPI_Datatype dataPointDatatype;
	int lengths[] = {TICKER_NAME_LENGTH,1,1,1,1};
	MPI_Aint types[] = {offsetof(dataPoint,name),offsetof(dataPoint,open),offsetof(dataPoint,adjClose),offsetof(dataPoint,percentChange),offsetof(dataPoint,time)};
	int disps[] = {MPI_CHAR,MPI_DOUBLE,MPI_DOUBLE,MPI_DOUBLE,MPI_LONG_LONG};
	MPI_Type_create_struct(5,lengths,types,disps,&dataPointDatatype);
	MPI_Type_commit(&dataPointDatatype);

	std::vector<dataPoint> masterDataPointCluster;
	std::vector<ticker> masterTickerCluster;
	std::string sourceFileName = "./us-shareprices-daily.txt";
	if(rank==0) buildDataPointsComponent(&sourceFileName,&masterDataPointCluster);
	standardizedThreadedTickerGroupingComponent(&masterDataPointCluster,&masterTickerCluster);
	
	twoTickerIncreaseAnalysisComponent(&masterTickerCluster);

	MPI_Finalize();
}
