#include <cuda.h>
#include <stdio.h>
#include <cublas.h>
#include <iostream>
#include <algorithm> 
#include <time.h>

//__device__ void swap(int i, int j) {
//	float t;
//	float *a=new float[];
//	t = a[i];
//	a[i] = a[j];
//	a[j] = t;
//}


 __device__ void insertion_sort(float* knnqueue, int* knnqueueind, int qpitch, int k, int threadId){
	 int n, o;
	float var;
	 for (o = threadId; o < k*qpitch+threadId; o += qpitch){
		 n = o;
		 while (n > threadId){
			 if (knnqueue[n] >knnqueue[n - qpitch]){

				 var = knnqueue[n];
				 knnqueue[n] = knnqueue[n - qpitch];
				 knnqueue[n - qpitch] = var;
			 }
			 n -= qpitch;

		 }
	 }

 }
__global__ void selection_of_knn(float *distlist, int *indlist,int listpitch, int k, int numofpoint, int refdimofpoint,
	float *knnqueue, int *knnqueueind,int qpitch, int m,volatile int* result)
//the visibility of memory operations on the result variable is ensured by declaring it as volatile
//From Nvidia documentation.
{
	int thx;
	int queueinit = 0;
	int mval=m;
	float locmax;
	int	j = 0;
	int varpitch=qpitch;
	int b,c;//divfact for the dividing the sequence in the second bitonic sort step, 
	//initialization to 2 for the second stage of sorting as we devide the sequence into two lists and so on 
	float var, var1;
	//for bitonic diffrent stage sorting
	int move = mval / 2;

	thx = threadIdx.x + blockIdx.x*blockDim.x;
	if (thx < numofpoint){

//queue initialization
		if (queueinit == 0){
			while (j < k){
				knnqueue[j*qpitch + thx] = distlist[j*listpitch + thx];
				//printf("knnqueueu: %d	%f\n", thx, knnqueue[j*qpitch + thx]);
				j++;
			}
			insertion_sort(knnqueue, 0, qpitch, k, thx);
			queueinit = 1;
			
		}
		//insertion in the first level m
		//locmax is the first element in the queue
		locmax = knnqueue[thx];
		for (int i = k*listpitch + thx; i < refdimofpoint*listpitch+thx; i += listpitch){
			//locmax = knnqueue[thx];//re_assigning the locmax to the head of the first level in the queue
			mval = m;
			move = mval / 2;//reinitializing mval ,move to deal with the remaining elemnets in the list
			if (distlist[i] < locmax){
				knnqueue[thx] =  distlist[i];
				
				//insert to the first level m
				insertion_sort(knnqueue, 0, qpitch, m, thx);
				locmax = knnqueue[thx];
				while((locmax < knnqueue[mval*qpitch + thx])/*&&(mval<=k)*/){				
						//first bitonic sort step(two sorted list in decreasing order)
						for (int a = mval*qpitch + thx; (a<k*qpitch+thx)&&(a < (2 * mval*qpitch) + thx); a += qpitch){
							if (knnqueue[a] > knnqueue[a - varpitch]){
								var = knnqueue[a];
								knnqueue[a] = knnqueue[a - varpitch];
								knnqueue[a - varpitch] = var;
							}
							else{
								break;//to finish the first bitonic step at the size of the previous level in the queue 
							}
							varpitch += 2 * qpitch;

						}//end of for
							//next bitonic sort steps
							while (move >0){
								for (c = 0; (c<(k*qpitch) + thx) && (c < (mval * 2 * qpitch)+thx); c += move * 2 * qpitch){
									/*if (thx == 0)
									printf("move,localmax,mval,move*2,b:	%d	%f	%d	%d	\n", move, locmax, mval, move * 2);*/
									for (b = thx; (b<k*qpitch + thx) && (b < move*qpitch + thx); b += qpitch){
										if ((knnqueue[b + c] < knnqueue[b + (move*qpitch) + c]) && (b + (move*qpitch) + c<k*qpitch+thx)){
											var1 = knnqueue[b+c];
											knnqueue[b+c] = knnqueue[b + (move*qpitch)+c];
											knnqueue[b + (move*qpitch)+c] = var1;
										
											/*if (thx == 0){
												for (int u = thx; u < k*qpitch; u += qpitch)
													printf("next bitonic: %d	%f\n", thx, knnqueue[u]);
												printf("\n");
											}*/
										}

										/*if (knnqueue[b + mval*qpitch] < knnqueue[b + (mval + move)*qpitch]){
											var2 = knnqueue[b + mval*qpitch];
											knnqueue[b + mval*qpitch] = knnqueue[b + (mval + move)*qpitch];
											knnqueue[b + (mval + move)*qpitch] = var2;
										}*/
										
									}
								}
								
								move /= 2;
							}//end of while	
							//to compare with the next level in the queue
							locmax = knnqueue[mval*qpitch+thx];//to ensure that the level heads are in decreasing order.
						/*	if (thx==0)
							printf("locmax:	%f\n", locmax);*/
							mval =mval* 2;
							move = mval / 2;
							varpitch = qpitch;
					}//the end of bitonic sort process for merging levels
			}
			locmax = knnqueue[thx];//re_assigning the locmax to the head of the first level in the queue
		}
		
		//bitonic merge
		//if the head of the second level is less than that of the first one of size m
	
	}//end of thx<numofpoint	
	/*if (thx == 66){
		for (int y = thx; y < (k*qpitch)+thx; y += qpitch){
			printf("last result: %d	%f\n", var2, knnqueue[y]);
			var2+=1;
		}
		printf("\n");

	}*/
	}



int main(){
	//testing width=8000; h=32000 k=64
	cudaEvent_t start, stop;
	cudaEventCreate(&start);
	cudaEventCreate(&stop);
	int width =8192;//query points
	int height =32768;//ref points 
	int k =512;
	size_t lpitch;
	size_t qpitch;
	cudaError_t val;

	float *d_indistqueue;
	float *d_list;
	float *l_in = new float[height*width];
	float *qh_in = new float[width*k];
	//allocate cpu memor
	float *h_out = (float *)malloc(width*k*sizeof(float));

	// generate the input array on the host/
	for (int i = 0; i < width*height; i++)
		l_in[i] = (float)rand() / (float)RAND_MAX;
		//l_in[i] = width*height- i;//should be used for testing bitonic sort
	//float l_in[20] = {20,21,16,14,22,23,24,8,30,32,0,5,4,3,2,1,0,6,2,7};
	val=cudaMallocPitch((void **) & d_indistqueue, &qpitch, width*sizeof(float), k);
	val=cudaMallocPitch((void **) &d_list, &lpitch, width*sizeof(float), height);
	if (val)
		printf("Memorypitch Error: %s\n", cudaGetErrorString(val));


	// transfer the array to the GPU
	cudaMemcpy2D(d_indistqueue, qpitch, qh_in, width*sizeof(float), width*sizeof(float), k, cudaMemcpyHostToDevice);
	cudaMemcpy2D(d_list, lpitch, l_in, width*sizeof(float), width*sizeof(float), height, cudaMemcpyHostToDevice);
	// launch the kernel
	dim3 Grid(width / 512 + 1, 1, 1);
	dim3 threads(512, 1);
	cudaEventRecord(start,0);
	selection_of_knn <<<Grid, threads>> >(d_list, 0,lpitch/sizeof(float), k, width, height, d_indistqueue, 0,qpitch/sizeof(float),8,0);
	cudaEventRecord(stop,0);
	cudaEventSynchronize(stop);

	//bitonic_finalize_col<<<1,1>>>(d_indistqueue,1,1,8);

	//
	// copy back the result array to the CPU
	cudaMemcpy2D(h_out,width*sizeof(float), d_indistqueue,qpitch ,width*sizeof(float),k, cudaMemcpyDeviceToHost);
	//cudaMemcpy(l_out, d_list, list_size*sizeof(int), cudaMemcpyDeviceToHost);
	float milliseconds;
	cudaEventElapsedTime(&milliseconds, start, stop);
	cudaEventDestroy(start);
	cudaEventDestroy(stop);
	//
	/*for (int j = 0; j < k*width; j++)
		printf("dequeue: %d	%f\n", j, h_out[j]);
*/
	printf("The required time:	%f\n", milliseconds / 1000);

	cudaFree(d_indistqueue);
	cudaFree(d_list);
	return 0;
}