
#include <cuda.h>
#include <stdio.h>
#include <cublas.h>
#include <iostream>
#include <algorithm> 
#include <time.h>

//sorting the elemens in decreasing order
__device__ void insertion_sort(float* knnqueue, int* knnqueueind, int qpitch, int k, int threadId){
	int n, o;
	float var;
	for (o = threadId; o < (k*qpitch)+threadId; o += qpitch){
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

__device__ void increasing_insertion_sort(float* knnqueue, int* knnqueueind, int qpitch, int k, int threadId){
	int n, o;
	float var;
	for (o = threadId; o < (k*qpitch)+threadId; o += qpitch){
		n = o;
		while (n > threadId){
			if (knnqueue[n]<knnqueue[n - qpitch]){

				var = knnqueue[n];
				knnqueue[n] = knnqueue[n - qpitch];
				knnqueue[n - qpitch] = var;
			}
			n -= qpitch;

		}
	}

}


__global__ void buffered_search(float *distlist, int *indlist, int listpitch,
	float *knnqueue, int *knnqueueind, int qpitch,int m,int k, int refnumber,
	int querynumber, float *dbuffer, int *ibuffer,int bsize,int bufpitch){
	int thx;
	thx = threadIdx.x + blockIdx.x*blockDim.x;
	int var2 = 0;
	int var3 = 0;
	volatile __shared__ int flag[128/32];
	int mval = m;
	int varpitch = qpitch;
	int buf_size_var = 0;
	int b, c;
	float var, var1;
	int move = mval / 2;
	int queueinit = 0;
	int j = 0;
	float locmax;
	int bufvar = thx;
	//int *flag = &flags[threadIdx.x / 32];
	if (thx < querynumber){
		
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
		locmax = knnqueue[thx];

		//Checking the list of distances
		/*var3 = 0;
		for (int i = thx; i < (refnumber*listpitch) + thx; i += listpitch){
			if ((thx == 4444) && (distlist[i] <= 0.298632)){
				printf("LIST22: %d	%f\n", var3, distlist[i]);
				var3++;
			}
		}*/
		for (int i = k*listpitch + thx; i < refnumber*listpitch + thx; i += listpitch){
			
			//bufvar = thx;
			//flag[threadIdx.x / 32] = 0;
				if (distlist[i] <= locmax){
					dbuffer[bufvar] = distlist[i];
					bufvar += bufpitch;
					buf_size_var++;
				}
			if (buf_size_var == bsize){
				flag[threadIdx.x / 32] = 1;
				/*buf_size_var = 0;
				bufvar = thx;*/

			}
			if (flag[threadIdx.x / 32] == 1){
				flag[threadIdx.x / 32] = 0;
				bufvar = thx;
				increasing_insertion_sort(dbuffer, 0, bufpitch, buf_size_var, thx);
				/*if (thx == 0){
					var2 = 0;
					for (int y = thx; y < (buf_size_var*bufpitch) + thx; y += bufpitch){
						printf("bffeer : %d	%f\n", var2, dbuffer[y]);
						var2 += 1;
					}
					printf("\n");

				}*/
				//Insert from Buffer to Merge Queue
				locmax = knnqueue[thx];

				for (int insert = thx; insert < buf_size_var*bufpitch + thx; insert += bufpitch){
					mval = m;
					move = mval / 2;//reinitializing mval ,move to deal with the remaining elemnets in the list
					if (dbuffer[insert] <=locmax){
						knnqueue[thx] = dbuffer[insert];

						//insert to the first level m
						insertion_sort(knnqueue, 0, qpitch, m, thx);
						locmax = knnqueue[thx];
						/*if (thx == 0){
							var2 = 0;
							for (int y = thx; y < (k*qpitch) + thx; y += qpitch){
								printf("before bitnic : %d	%f\n", var2, knnqueue[y]);
								var2 += 1;
							}
							printf("\n");

						}*/
						while ((locmax < knnqueue[mval*qpitch + thx]) && (mval <= k)){
							//first bitonic sort step(two sorted list in decreasing order)
							for (int a = mval*qpitch + thx; (a < k*qpitch + thx) && (a < (2 * mval*qpitch) + thx); a += qpitch){
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
							while (move > 0){
								for (c = 0; (c < (k*qpitch) + thx) && (c < (mval * 2 * qpitch) + thx); c += move * 2 * qpitch){
									for (b = thx; (b < k*qpitch + thx) && (b < move*qpitch + thx); b += qpitch){
										if ((knnqueue[b + c] < knnqueue[b + (move*qpitch) + c]) && (b + (move*qpitch) + c < k*qpitch + thx)){
											var1 = knnqueue[b + c];
											knnqueue[b + c] = knnqueue[b + (move*qpitch) + c];
											knnqueue[b + (move*qpitch) + c] = var1;
										}

									}
								}

								move /= 2;
							}//end of while	
							//to compare with the next level in the queue
							locmax = knnqueue[mval*qpitch + thx];//to ensure that the level heads are in decreasing order.
							mval = mval * 2;
							move = mval / 2;
							varpitch = qpitch;
						}//the end of bitonic sort process for merging levels
					}
					else{
						break;
					}
					locmax = knnqueue[thx];//re_assigning the locmax to the head of the first level in the queue
				}//end for (int insert = thx; insert < bsize*bufpitch; insert += bufpitch)				
				buf_size_var = 0;

			}	
			locmax = knnqueue[thx];
		}//end of looping through distlist
	}
	if (thx ==0){
		var2 = 0;
		for (int y = thx; y < (k*qpitch) + thx; y += qpitch){
			printf("last result: %d	%f\n", var2, knnqueue[y]);
			var2 += 1;
		}
		printf("\n");

	}
}


int main(){
	//testing width=8000; h=32000 k=64
	cudaEvent_t start, stop;
	cudaEventCreate(&start);
	cudaEventCreate(&stop);
	int width =8192;//query points
	int height = 32768;//ref points 
	int k =1024;
	int bsize =16;
	//float var4[32] = { 20, 21, 16, 14, 22, 23, 24, 1,1,1,2,2,3,3,3,2,2,1,1,3,4,4,2,1,1,0,0,0,0,3,1,3 };
	size_t lpitch;
	size_t qpitch;
	size_t bufpitch;
	cudaError_t val1,val2,val3;

	float *d_dbuffer;
	float *d_indistqueue;
	float *d_list;
	float *h_dbuffer = new float[width*bsize];
	float *qh_in = new float[width*k];
	float *l_in = new float[height*width];

	//allocate cpu memor
	float *h_out = (float *)malloc(width*k*sizeof(float));

	// generate the input array on the host/
	for (int i = 0; i < width*height; i++){
		l_in[i] = ((float)rand() / (float)RAND_MAX) * 10;
	}
	//l_in[i] = width*height - i;//should be used for testing bitonic sort
	//l_in = var4;
	val1 = cudaMallocPitch((void **)&d_dbuffer, &bufpitch, width*sizeof(float), bsize);

	val2 = cudaMallocPitch((void **)& d_indistqueue, &qpitch, width*sizeof(float), k);
	val3 = cudaMallocPitch((void **)&d_list, &lpitch, width*sizeof(float), height);
	if (val1)
		printf("Memorypitch Error For buffer: %s\n", cudaGetErrorString(val1));
	if (val2)
		printf("Memorypitch Error for queue: %s\n", cudaGetErrorString(val1));
	if (val3)
		printf("Memorypitch Error for list: %s\n", cudaGetErrorString(val1));

	// transfer the array to the GPU

	cudaMemcpy2D(d_dbuffer, bufpitch, h_dbuffer, width*sizeof(float), width*sizeof(float), bsize, cudaMemcpyHostToDevice);
	cudaMemcpy2D(d_indistqueue, qpitch, qh_in, width*sizeof(float), width*sizeof(float), k, cudaMemcpyHostToDevice);
	cudaMemcpy2D(d_list, lpitch, l_in, width*sizeof(float), width*sizeof(float), height, cudaMemcpyHostToDevice);
	// launch the kernel
	dim3 Grid(width / 128 + 1, 1, 1);
	dim3 threads(128, 1);
	cudaEventRecord(start, 0);

	buffered_search <<<Grid, threads >> >(d_list, 0, lpitch / sizeof(float), d_indistqueue, 0, 
		qpitch/sizeof(float),8, k, height,width, d_dbuffer,0,bsize,bufpitch/sizeof(float));

	cudaEventRecord(stop, 0);
	cudaEventSynchronize(stop);
	// make the host block until the device is finished with foo
	cudaDeviceSynchronize();

	// check for error
	cudaError_t error = cudaGetLastError();
	if (error != cudaSuccess)
	{
		// print the CUDA error message and exit
		printf("CUDA error: %s\n", cudaGetErrorString(error));
		exit(-1);
	}
	// copy back the result array to the CPU
	cudaMemcpy2D(h_out, width*sizeof(float), d_indistqueue, qpitch, width*sizeof(float), k, cudaMemcpyDeviceToHost);
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

