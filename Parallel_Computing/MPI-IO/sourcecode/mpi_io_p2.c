
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

/* error check function */
#define MPI_CHECK(fn) { int errcode; errcode = (fn); \
         if (errcode != MPI_SUCCESS) handle_error(errcode, #fn ); }
static void handle_error(int errcode, char *str)
{
 	char msg[MPI_MAX_ERROR_STRING];
 	int resultlen;
 	MPI_Error_string(errcode, msg, &resultlen);
 	fprintf(stderr, "%s: %s\n", str, msg);
 	MPI_Abort(MPI_COMM_WORLD, 1);
}
 

/* main function */
int main(int argc, char *argv[])
{
	/* 3 command line parameters: [common file],[data_size for each rank],[time_result]*/
	if(argc!=4){
		printf("Usage: %s [output filename] [data_size] [resultint timing]\n",argv[0]);    
    		exit(0);
	}
	char *filename = argv[1];
	int mb=atoi(argv[2]);
	char *result_file=argv[3];
	/* data size for each rank */
	int data=mb*1000*1000/4;

	/********** init the mpi*************/
	int size, rank;
	MPI_Init(NULL,NULL);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_File fh;
    MPI_Status status;
	int write_buf[data];//write buffer array
	int i;
	/* assign the random value to write buffer array*/
	srand((unsigned) time(NULL));
	for(i=0;i<data;i++){
		write_buf[i]=rand()%10000;
	}
	double overall_time=MPI_Wtime();
	double run_time,write_time,read_time,total_time;//define the time variables.
	double bandwidth_write,bandwidth_read;

	MPI_CHECK(MPI_Barrier( MPI_COMM_WORLD ));//each rank stops at this point to get the same start time of write
	/* write start */
	run_time=MPI_Wtime();
	//open a common file 
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh));
	//determin the position(offset) to write
	MPI_Offset offset = rank*sizeof(int)*data;
	//set the view for each rank with the offset
	MPI_CHECK(MPI_File_set_view(fh, offset, MPI_INT, MPI_INT, "native", MPI_INFO_NULL));
	//write the data to the position of the common file with the view set before
	MPI_CHECK(MPI_File_write_at(fh, 0, write_buf, data, MPI_INT, &status));
	//finish write, close the file.
	MPI_CHECK(MPI_File_close(&fh));

	run_time=MPI_Wtime()-run_time;//calculate the write time for each rank
	/* write end */

	/* get the maximum write time in ranks*/
    MPI_CHECK(MPI_Reduce(&run_time, &write_time, 1, MPI_DOUBLE, MPI_MAX, 0,MPI_COMM_WORLD));
    /* calculate the bandwith=(number of ranks)*(data size of each rank)/writetime */    
    bandwidth_write=size*mb/write_time;
    if (rank == 0) {
      printf("Write time:  %lf seconds\nwrite_bandwidth: %f MB/s\n",write_time,bandwidth_write);
    }

    MPI_CHECK(MPI_Barrier( MPI_COMM_WORLD )); //each rank stops at this point to get the same start time of read
	/* read start */
	run_time=MPI_Wtime();

	//open the common file to read
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh));
	
	//set the view for each rank to view
	MPI_CHECK(MPI_File_set_view(fh, offset, MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL));
	char read_buf[data];//the read buffer array.
	
	//read the data on the positon of the common file with the view set before
	MPI_CHECK(MPI_File_read_at(fh, 0, read_buf, data, MPI_INT, &status));
	
	// finish reading and close the file.
	MPI_CHECK(MPI_File_close(&fh));

	run_time=MPI_Wtime()-run_time; //calculate the read time for each rank.
	/* read finished */

	/* get the maximum read time in ranks */			
    MPI_CHECK(MPI_Reduce(&run_time, &read_time, 1, MPI_DOUBLE, MPI_MAX, 0,MPI_COMM_WORLD));
    /* calculate the bandwith=(number of ranks)*(data size of each rank)/readtime */
    bandwidth_read=size*mb/read_time;
    if (rank == 0) {
      printf("Read time:  %lf seconds\nread bandwidth: %f MB/s\n",read_time,bandwidth_read);
    }

    overall_time=MPI_Wtime()-overall_time;//the overall time 
    if (rank == 0) {
      printf("overall_time:  %lf seconds\n",overall_time);
    }
  
    /*write the time result to file*/
    if(rank==0){
    	FILE *file;
	    if((file=fopen(result_file,"wt"))==NULL)
	    {
	        perror("fopen");
	        exit(1);
	    }
	    fprintf(file,"Write time:  %lf seconds\nRead time:  %lf seconds\noverall_time:  %lf seconds\nwrite bandwidth:  %f MB/s\nread bandwidth:  %f MB/s\n",write_time,read_time,overall_time,bandwidth_write,bandwidth_read);
 		fclose(file);
    }
   
    /* DELETE THE TEMP FILE*/
 	MPI_File_delete(filename, MPI_INFO_NULL);  

	MPI_Finalize();
	/********MPI END*********/

	return 0;
}