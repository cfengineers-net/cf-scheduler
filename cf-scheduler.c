/*
   cf-scheduler.c

   Copyright (C) cfengineers.net

   Written and maintained by Jon Henrik Bjornstad <jonhenrik@cfengineers.net>

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; version 3.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
*/

//#define _MULTI_THREADED
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>

#ifdef __linux__
# include <linux/un.h>
# include <linux/stat.h>
#else 
# include <sys/un.h>
# include <sys/stat.h>
# define UNIX_PATH_MAX 108
#endif

#define BUFSIZE 4096
#define CF_MAXTHREADS 128
#define SOCKPATH "/var/tmp/cf-scheduler-socket"

int num_threads = 0;
int run_status[CF_MAXTHREADS] = {};
int job_ids = 0;
short debug = 0;
pthread_mutex_t mutex;
pthread_cond_t cond;
char *prog_name_canon = "cf_scheduler";

typedef struct {
	short *run_it;
	int interval;
	char *label;
	char *job;
	int job_id;
	pthread_attr_t pta;
	pthread_t thread;
} Job;

typedef struct {
	short run_it;
	int index;
	char *job;
	char *label;
	int interval;
} Job_args;

void usage() {
	printf("\n"
"This is the usage\n"
"\n");
}

void dbg_printf(const char *fmt, ...) {
	if(debug == 1){
		va_list args;
		va_start(args, fmt);
		vfprintf(stderr, fmt, args);
		va_end(args);
	}
}

int exec_shell(const char *cmd) {
	FILE *p = NULL;

	if ((p = popen(cmd, "w")) == NULL)
		return (-1);

	return (pclose(p));
}

void *run(void *job) {
	Job_args *j = (Job_args *)job;
	struct timeval before,after,sleeper;
	FILE *fp;
	long msec; 
	char buf[BUFSIZE];

	errno = 0;

	while(j->run_it > 0){
		gettimeofday(&before, NULL);		
	
		exec_shell(j->job);

		gettimeofday(&after, NULL);

		msec = (j->interval*1000000L) - (((after.tv_sec - before.tv_sec)*1000000L + after.tv_usec) - before.tv_usec);

		dbg_printf("Run_it: %d,Label: %s, Job: %s => Will sleep for : %ld microseconds\n", j->run_it, j->label, j->job, msec);

		if(msec > 0)
			u_sleep(msec);
	}
	dbg_printf("Exiting thread....\n");
	free(j);

	pthread_exit(NULL);
}

int u_sleep(long usec) {
	struct timeval tv;
	tv.tv_sec = usec/1000000L;
	tv.tv_usec = usec%1000000L;
	return select(0, 0, 0, 0, &tv);
}

void create_job(Job *jobs, int interval, char *label, char *command){

	int rc = 0;
	size_t stack_size;
	Job_args *j_args = (Job_args*) malloc(sizeof(Job_args));

	j_args->job = (char *) calloc(strlen(command) + 1,sizeof(char));
	j_args->label = (char *) calloc(strlen(label) + 1,sizeof(char));

	strcpy(j_args->label,label);
	strcpy(j_args->job,command);

	j_args->interval = interval;

	jobs[num_threads].label = (char *) calloc(strlen(label) + 1,sizeof(char));
	jobs[num_threads].job = (char *) calloc(strlen(command) + 1,sizeof(char));

	strcpy(jobs[num_threads].label,label);
	strcpy(jobs[num_threads].job,command);

	jobs[num_threads].interval = interval;
	jobs[num_threads].job_id = job_ids + 1;

	j_args->run_it = 1;
	jobs[num_threads].run_it = &j_args->run_it;

	rc = pthread_create(&jobs[num_threads].thread, NULL, run, (void *)j_args);

	num_threads++;
	job_ids++;
}

int locate_job(Job *jobs, char *label, int job_id){
	int i = 0;
	for(i = 0; i < num_threads; i++) {
		if(job_id > 0 && jobs[i].job_id == job_id){
			return i;	
		}else if(label != NULL && strcmp(jobs[i].label,label) == 0){
			return i;
		}
	}
	return -1;
}

int connection_handler(Job *jobs, int connection_fd) {
	int nbytes;
	char buffer[BUFSIZE];
	nbytes = read(connection_fd, buffer, BUFSIZE);
	buffer[nbytes] = 0;
	char op[BUFSIZE];
	char command[BUFSIZE];
	int interval,job_id;
	int thread_wait = 0;
	char label[BUFSIZE];
	Job *j;
	
	if(sscanf(buffer, "op=job intvl=%d lbl=%s cmd=%[^\t\n] %*s",  &interval, label, command)) {
		if((locate_job(jobs,label,0)) > -1){
			nbytes = snprintf(buffer, BUFSIZE,"+%s_exists\n", label);
			write(connection_fd, buffer, nbytes);
		} else {
			create_job(jobs,interval,label,command);
			nbytes = snprintf(buffer, BUFSIZE, "+%s_repaired\n", label);
			write(connection_fd, buffer, nbytes);
		}
	}else if(sscanf(buffer, "op=status%*s")) {

		nbytes = snprintf(buffer, BUFSIZE,"=num_threads=%d\n", num_threads);
		write(connection_fd, buffer, nbytes);

		int i = 0;

		for(i = 0; i < num_threads; i++){
			if(jobs[i].label != NULL){
				nbytes = snprintf(buffer, BUFSIZE,"=status[%d][cmd]=%s\n", jobs[i].job_id, jobs[i].job);
				write(connection_fd, buffer, nbytes);
				nbytes = snprintf(buffer, BUFSIZE,"=status[%d][interval]=%d\n", jobs[i].job_id, jobs[i].interval);
				write(connection_fd, buffer, nbytes);
				nbytes = snprintf(buffer, BUFSIZE,"=status[%d][label]=%s\n", jobs[i].job_id, jobs[i].label);
				write(connection_fd, buffer, nbytes);
			}
		}
	} else if((strstr(buffer, "op=term")) != NULL) { //sscanf(buffer, "op=term%*s")){
		int index = -1;
		int lab = 0;
		int id = 0;
		if(sscanf(buffer, "op=term job_id=%d", &job_id)){
			dbg_printf("Request for terminating job %d\n", job_id);	
			index = locate_job(jobs, NULL, job_id);
			id = 1;
		}else if(sscanf(buffer, "op=term lbl=%s", label)){
			dbg_printf("Request for terminating job with label %s\n", label);	
			index = locate_job(jobs, label, 0);
			lab = 1;
		}
		if(index > -1){
			dbg_printf("Found job with index %d\n", index);	
			short *t = jobs[index].run_it;
			*t = 0;
			thread_wait = 1;
			int i;
			for (i = index; i < num_threads - 1;i++) {
				jobs[i] = jobs[i + 1];
			}
			num_threads--;

		}
		if(lab > 0)	
			if(index > -1)
				nbytes = snprintf(buffer, BUFSIZE, "+%s_label_terminated\n", label);
			else
				nbytes = snprintf(buffer, BUFSIZE, "+%s_label_notfound\n", label);
		else if(id > 0)
			if(index > -1)
				nbytes = snprintf(buffer, BUFSIZE, "+%d_id_terminated\n", job_id);
			else
				nbytes = snprintf(buffer, BUFSIZE, "+%d_id_notfound\n", job_id);

			write(connection_fd, buffer, nbytes);
	}
	close(connection_fd);
	return 0;
}

int send_command(char *opstring){
	
	struct sockaddr_un address;
	int socket_fd, nbytes;
	char buffer[BUFSIZE];

	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0) {
		//dbg_printf("socket() failed\n");
		return 1;
	}

	 /* start with a clean address structure */
	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, UNIX_PATH_MAX, SOCKPATH);

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0) {
		//dbg_printf("connect() failed\n");
		return 1;
	}

	nbytes = strlen(opstring);
	write(socket_fd, opstring, nbytes);

	while((nbytes = read(socket_fd, buffer, BUFSIZE-1)) != 0) {
		buffer[nbytes] = 0;
		printf("%s", buffer);
	}
	close(socket_fd);
}


int main(int argc, char *argv[]) {
	Job jobs[CF_MAXTHREADS];	
	int i;

	int rc = 0;
	
	struct sockaddr_un address;
	int socket_fd, connection_fd;
	socklen_t address_length;
	pid_t child;

	int status = 0;
	char *label = NULL;
	char *intrvl = NULL;
	char *command = NULL;
	char *job_id = NULL;
	short term = 0;
	int c = 0;
	struct stat fileStat;

	while ((c = getopt (argc, argv, "hdsti:l:c:I:")) != -1)
		switch (c) {
			case 's':
				status = 1;
				break;
			case 'l':
				label = optarg;
				break;
			case 'i':
				intrvl = optarg;
				break;
			case 'c':
				command = optarg;
				break;
			case 't':
				term = 1;
				break;
			case 'I':
				job_id = optarg;
				break;
			case 'd':
				debug = 1;
				break;
			case 'h':
				usage();
				exit(1);
			default:
				printf("ERROR: Unknown option '-%c'\n", optopt);
				usage();
				exit(1);
		}
	

	if(status > 0) {
		send_command("op=status");
		exit(0);
	}
	
	if(label != NULL && intrvl != NULL && command != NULL){
		char buf[BUFSIZE];
		sprintf(buf, "op=job intvl=%s lbl=%s cmd=%s",intrvl,label,command);
		send_command(buf);
		exit(0);
	}

	if(term > 0){
		char buf[BUFSIZE];
		if(job_id != NULL)
			sprintf(buf, "op=term job_id=%s",job_id);
		else if(label != NULL)
			sprintf(buf, "op=term lbl=%s",label);
		else
			exit(1);
		send_command(buf);
		exit(0);
	}

	if((stat(SOCKPATH,&fileStat)) > -1){
		printf("Socket %s exists, another process might be running. Exiting...\n", SOCKPATH);
		exit(1);
	}

	if(debug == 0) {
		if( (child=fork())<0 ) { //failed fork
			fprintf(stderr,"error: failed fork\n");
			exit(EXIT_FAILURE);
		}
		if (child>0) { //parent
			exit(EXIT_SUCCESS);
		}
		if( setsid()<0 ) { //failed to become session leader
			fprintf(stderr,"error: failed setsid\n");
			exit(EXIT_FAILURE);
		}
	}

	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0) {
		dbg_printf("socket() failed\n");
		return 1;
	} 

	//unlink(SOCKPATH);

	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, UNIX_PATH_MAX, SOCKPATH);

	if(bind(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0) {
		dbg_printf("bind() failed\n");
		return 1;
	}

	if(listen(socket_fd, 5) != 0) {
		dbg_printf("listen() failed\n");
		return 1;
	}

	char mode[5];
	strcpy(mode, "0700");
	int m = atoi(mode);
	chmod(SOCKPATH,S_IRUSR|S_IWUSR|S_IXUSR);

	while(1) {
		address_length = sizeof(address);
		if((connection_fd = accept(socket_fd, (struct sockaddr *) &address, &address_length)) == -1) {
			printf("accept error: %s\n", strerror(errno));
			break;
		}
		connection_handler(jobs, connection_fd);
	}

	close(socket_fd);
	unlink(SOCKPATH);
	return(0);
}

