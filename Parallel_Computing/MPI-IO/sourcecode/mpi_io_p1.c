
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

/*error check*/
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
 

/*main function*/
int main(int argc, char *argv[])
{
	char *filename;
	filename=argv[1];
	if(argc!=2){
		printf("Usage: %s [output_filename]\n",argv[0]);    
    		exit(0);
	}//command line parameters: output_filename 

	/*************initial the MPI **************/
	int size, rank;
	MPI_Init(NULL,NULL);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_File fh;
    MPI_Status status;

	int buf[10];
	int i;
	for(i=0;i<10;i++){
		buf[i]=rank;
	}//assign the rank to the buffer array.
	
	//each rank open a common file.
	MPI_CHECK(MPI_File_open(MPI_COMM_WORLD, filename, MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &fh));
	// assign the view position to each rank
	int view = rank*sizeof(int) * 10; 
	MPI_CHECK(MPI_File_set_view(fh, view, MPI_INT, MPI_INT, "native", MPI_INFO_NULL));
	//each rank write to the common file
	MPI_CHECK(MPI_File_write_at(fh, 0, buf, 10, MPI_INT, &status));
	//close the file
	MPI_CHECK(MPI_File_close(&fh));
	MPI_Finalize();
	/***************MPI end**************/
	return 0;
}