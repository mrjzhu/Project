Instruction:

First we need to upload the two files to jarvis
scp gauss_pthread gauss_mpi jzhu57@jarvis.cs.iit.edu:~

1. For Pthread
Totally there are 4 parameters [Matrix_size] [random_seed] [thread_number] [output_filename]

the example to run the program is: 
Matrix_size=2000,random_seed=1, thread_number=2, output_filename=output.txt
1) ”gcc -w -o gauss_pthread gauss_pthread.c -lpthread”
2) “gcc ./gauss_pthread 2000 1 2 output.txt ”

2. For MPI
Totally there are 3 parameters [Matrix_size] [random_seed] [thread_number]

the example to run the program is:
Matrix_size=2000,random_seed=1, output_filename=output.txt,
1) “mpicc -w -o gauss_mpi gauss_mpi.c”
2) ”mpiexec -n 4 ./gauss_mpi 2000 1 output.txt

ps: In my mpi method, the rank 0 did not do the parallel computing, so when run
the program, with 4 processors, the n should be 5


