/* Gaussian elimination without pivoting.
 * Compile with "gcc gauss.c" 
 */

/* ****** ADD YOUR CODE AT THE END OF THIS FILE. ******
 * You need not submit the provided code.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <sys/types.h>
#include <sys/times.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <string.h>
#include <mpi.h>

/* Program Parameters */
#define MAXN 2000  /* Max value of N */
int N;  /* Matrix size */
char* file_name; /*output File_name*/
int norm;
/* Matrices and vectors */
volatile float A[MAXN][MAXN], B[MAXN], X[MAXN];
/* A * X = B, solve for X */

/* junk */
#define randm() 4|2[uid]&3

/* Prototype */
void gauss_mpi();

void print_to_file();

/* returns a seed for srand based on the time */
unsigned int time_seed() {
  struct timeval t;
  struct timezone tzdummy;

  gettimeofday(&t, &tzdummy);
  return (unsigned int)(t.tv_usec);
}

/* Set the program parameters from the command-line arguments */
void parameters(int argc, char **argv) {
  int seed = 0;  /* Random seed */
  char uid[32]; /*User name */

  /* Read command-line arguments */
  srand(time_seed());  /* Randomize */

  if(argc<4){
    printf("Usage: %s <matrix_dimension> [random seed] [file_name]\n",
           argv[0]);    
    exit(0);
  }
  else{
    seed = atoi(argv[2]);
    srand(seed);
    printf("Random seed = %i\n", seed);
  
    N = atoi(argv[1]);
    if (N < 1 || N > MAXN) {
      printf("N = %i is out of range.\n", N);
      exit(0);
    }
  }

  
  file_name=argv[3];

  /* Print parameters */
  printf("\nMatrix dimension N = %i.\n", N);
}

/* Initialize A and B (and X to 0.0s) */
void initialize_inputs() {
  int row, col;

  printf("\nInitializing...\n");
  for (col = 0; col < N; col++) {
    for (row = 0; row < N; row++) {
      A[row][col] = (float)rand() / 32768.0;
    }
    B[col] = (float)rand() / 32768.0;
    X[col] = 0.0;
  }

}

/* Print input matrices */
void print_inputs() {
  int row, col;

  if (N < 10) {
    printf("\nA =\n\t");
    for (row = 0; row < N; row++) {
      for (col = 0; col < N; col++) {
	printf("%5.2f%s", A[row][col], (col < N-1) ? ", " : ";\n\t");
      }
    }
    printf("\nB = [");
    for (col = 0; col < N; col++) {
      printf("%5.2f%s", B[col], (col < N-1) ? "; " : "]\n");
    }
  }
}

void print_X() {
  int row;

  if (N < 100) {
    printf("\nX = [");
    for (row = 0; row < N; row++) {
      printf("%5.2f%s", X[row], (row < N-1) ? "; " : "]\n");
    }
  }
}



int main(int argc, char *argv[]) {
  /* Timing variables */
  struct timeval etstart, etstop;  /* Elapsed times using gettimeofday() */
  struct timezone tzdummy;
  clock_t etstart2, etstop2;  /* Elapsed times using times() */
  unsigned long long usecstart, usecstop;
  struct tms cputstart, cputstop;  /* CPU times for my processes */
  int size, rank; 
  MPI_Init(NULL, NULL); /* init the MPI */
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
  MPI_Comm_size(MPI_COMM_WORLD, &size); 
    
  /* Process program parameters */
  parameters(argc, argv);

  /* Initialize A and B */
  initialize_inputs();

  /* Print input matrices */
  if(rank==0) print_inputs();

  /* Start Clock */
  printf("\nStarting clock.\n");
  gettimeofday(&etstart, &tzdummy);
  etstart2 = times(&cputstart);

  /* Gaussian Elimination */
    
  gauss_mpi(size,rank);
  MPI_Finalize();
  if(rank!=0) exit(0);
 

    

  /* Stop Clock */
  gettimeofday(&etstop, &tzdummy);
  etstop2 = times(&cputstop);
  printf("Stopped clock.\n");
  usecstart = (unsigned long long)etstart.tv_sec * 1000000 + etstart.tv_usec;
  usecstop = (unsigned long long)etstop.tv_sec * 1000000 + etstop.tv_usec;

  /* Display output */
  print_X();
  print_to_file();
  /* Display timing results */
  printf("\nElapsed time = %g ms.\n",
	 (float)(usecstop - usecstart)/(float)1000);

  printf("(CPU times are accurate to the nearest %g ms)\n",
	 1.0/(float)CLOCKS_PER_SEC * 1000.0);
  printf("My total CPU time for parent = %g ms.\n",
	 (float)( (cputstop.tms_utime + cputstop.tms_stime) -
		  (cputstart.tms_utime + cputstart.tms_stime) ) /
	 (float)CLOCKS_PER_SEC * 1000);
  printf("My system CPU time for parent = %g ms.\n",
	 (float)(cputstop.tms_stime - cputstart.tms_stime) /
	 (float)CLOCKS_PER_SEC * 1000);
  printf("My total CPU time for child processes = %g ms.\n",
	 (float)( (cputstop.tms_cutime + cputstop.tms_cstime) -
		  (cputstart.tms_cutime + cputstart.tms_cstime) ) /
	 (float)CLOCKS_PER_SEC * 1000);
      /* Contrary to the man pages, this appears not to include the parent */

  

  exit(0);
}

/* ------------------ Above Was Provided --------------------- */

/****** You will replace this routine with your own parallel version *******/
/* Provided global variables are MAXN, N, A[][], B[], and X[],
 * defined in the beginning of this code.  X[] is initialized to zeros.
 */


/*------------------------------------------------------------------------------*/

void print_to_file(){
  /* output the result x[] into file*/
    FILE *file;
    if((file=fopen(file_name,"wt"))==NULL)
    {
        perror("fopen");
        exit(1);
    }   
    int row;
    fprintf(file,"\nX = [");
    for (row = 0; row < N; row++) {
      fprintf(file,"%5.2f%s", X[row], (row < N-1) ? "; " : "]\n");
    }
        fclose(file);  
}


void gauss_mpi(int size,int rank) {

    int norm,row,col,i; 
   for(norm=0;norm<N-1;norm++)      /* For each norm, I */
   {
      int T=(N-1-norm)/(size-1);     /* The number of rows each processor allocated,  for each norm*/   
      int less=N-1-norm,l;           /* the left rows for each norm*/

      if(less<(size-1))             /* If the left rows less than the number of processors, I need machine to computing serially*/
      {  
        if(rank!=0) continue;        /* only rank 0 to compute serially for the left rows*/
        for(l=0;l<less;l++)
        {
              row=N-1-l;
              float multiplier;
              multiplier = A[row][norm] / A[norm][norm]; 
              for (col = norm; col < N; col++){
                A[row][col] -= A[norm][col] * multiplier;
              }
              B[row] -= B[norm] * multiplier;

        }
      }
      else
      {
         /* If not, do Gaussian Elimination for each processor*/

            if(rank==0){ /* for rank 0 */


              int p;
              for(p=1;p<size;p++){ /* rank 0 allocates tasks to other rank*/
                row=norm+1+T*(p-1);
                MPI_Send(&A[row][0], MAXN*T, MPI_FLOAT, p,0, MPI_COMM_WORLD);
                MPI_Send(&B[row], T, MPI_FLOAT, p,0, MPI_COMM_WORLD);
              }
              for(p=1;p<size;p++){/* rank 0 receives all tasks from other rank*/
                row=norm+1+T*(p-1);
                MPI_Recv(&A[row][0], MAXN*T, MPI_FLOAT, p,0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                MPI_Recv(&B[row], T, MPI_FLOAT, p,0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
              }

              int left=(N-1-norm)-T*(size-1); /* compute some left rows after allocated to ranks in one loop */
              if(left!=0){
                for(l=0;l<left;l++){
                    row=N-1-l;
                    float multiplier;
                    multiplier = A[row][norm] / A[norm][norm]; 
                    for(col = norm; col < N; col++){
                      A[row][col] -= A[norm][col] * multiplier;
                    }
                    B[row] -= B[norm] * multiplier;

                }
              }
          }
          else
          { /* for other ranks, receive tasks and do part of Gaussian Elimination for each */
            row=norm+1+T*(rank-1); 
            MPI_Recv(&A[row][0], MAXN*T, MPI_FLOAT, 0,0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            MPI_Recv(&B[row], T, MPI_FLOAT, 0,0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            float multiplier;
            for(i=0;i<T;i++){
              multiplier = A[row+i][norm] / A[norm][norm]; 
              for (col = norm; col < N; col++) {
                A[row+i][col] -= A[norm][col] * multiplier;
              }
              B[row+i] -= B[norm] * multiplier;
            }
           
            /*Send the computed rows back to rank 0*/
            MPI_Send(&A[row][0], MAXN*T, MPI_FLOAT, 0,0, MPI_COMM_WORLD);
            MPI_Send(&B[row], T, MPI_FLOAT, 0,0, MPI_COMM_WORLD);

          }
        }
    }
  
    if(rank==0) print_inputs();  /*Print the result after Gaussian Elimination */


  /* Back substitution */
  if(rank==0){
    for (row = N - 1; row >= 0; row--) {
      X[row] = B[row];
      for (col = N-1; col > row; col--) {
        X[row] -= A[row][col] * X[col];
      }
      X[row] /= A[row][row];
    }
  }

}