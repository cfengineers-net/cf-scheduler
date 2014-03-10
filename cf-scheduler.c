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
#include <assert.h>
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
#define READ 0
#define WRITE 1

struct job {
	short run_it;
	int interval;
	char *label;
	char *cmd;
	int job_id;
	int in;
	int out;
	pid_t pid;
	pthread_attr_t pta;
	pthread_t thread;
};

typedef struct job Job;

struct job_node {
	Job *job;
	struct job_node *next;
};

typedef struct job_node Job_node;

struct job_list {
	Job_node *head;
	Job_node *tail;
	pthread_mutex_t mutex;
};

typedef struct job_list Job_list;

int num_threads = 0;
int job_id = 0;
short debug = 0;
struct sigaction sigact;
char *prog_name_canon = "cf_scheduler";
int socket_fd = 0;
static void *job_l;
//Job_list *jobs = NULL;


void usage() {
	printf("\n"
"Usage: cf-scheduler [-c command] [-l label] [-i interval] [-s] [-t] [-I job_id] [-d] [-h]\n"
"\n"
"A multithreaded scheduler that outputs return and status values in CFEngine module\n"
"format.\n"
"\n" 
"  -c       Command to periodically run.\n"
"  -i       Interval for job.\n"
"  -l       Label for command. Used either on termination or initiation. Needs to be unique.\n"
"  -h       File to write encrypted/decrypted contents to. '-' writes to stdout.\n"
"  -I       Job id to terminate\n"
"  -h       Print help.\n"
"  -t       Terminate a job. Needs to be used together with -l or -I.\n"
"  -s       Print current status.\n"
"  -d       Run daemon in foreground with debug messages.\n"
"\n"
"Written and maintained by Jon Henrik Bjornstad <jonhenrik@cfengineers.net>\n"
"\n"
"Copyright (C) cfengineers.net\n"
"\n");
}


void dbg_printf(const char *fmt, ...) {
	if(debug == 1){
		va_list args;
		va_start(args, fmt);
		fprintf(stderr, "Debug: ");
		vfprintf(stderr, fmt, args);
		va_end(args);
	}
}

pid_t popen2(const char *command, int *infp, int *outfp) {

    int p_stdin[2], p_stdout[2];
    pid_t pid;

    if (pipe(p_stdin) != 0 || pipe(p_stdout) != 0)
        return -1;

    pid = fork();

    if (pid < 0)
        return pid;
    else if (pid == 0)
    {
        close(p_stdin[WRITE]);
        dup2(p_stdin[READ], READ);
        close(p_stdout[READ]);
        dup2(p_stdout[WRITE], WRITE);
				setsid();
        execl("/bin/sh", "sh", "-c", command, NULL);
        perror("execl");
        exit(1);
    }

    if (infp == NULL)
        close(p_stdin[WRITE]);
    else
        *infp = p_stdin[WRITE];

    if (outfp == NULL)
        close(p_stdout[READ]);
    else
        *outfp = p_stdout[READ];

    return pid;
}

void *run(void *job) {
	Job *j = (Job *)job;
	struct timeval before,after,sleeper;
	FILE *fp;
	long msec; 
	char buf[BUFSIZE];

	errno = 0;
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,NULL);
	while(1){
		gettimeofday(&before, NULL);		

		dbg_printf("Spawning  => Label: %s, Job: %s\n", j->label, j->cmd);
	
		//exec_shell(j->cmd);
		j->pid = popen2(j->cmd,NULL,NULL);

		waitpid(j->pid,NULL,0);


		gettimeofday(&after, NULL);

		msec = (j->interval*1000000L) - (((after.tv_sec - before.tv_sec)*1000000L + after.tv_usec) - before.tv_usec);

		dbg_printf("Completed => Label: %s, Job: %s\n", j->label, j->cmd);

		dbg_printf("Sleep     => Label: %s, Job: %s (%ld mus)\n", j->label, j->cmd, msec);
		if(msec > 0)
			u_sleep(msec);
	}

	pthread_exit(NULL);
}

void add_job(Job_list *jobs, char *label, char *cmd, int interval){
	Job_node *temp, *current;
	int rc = 0;

	pthread_mutex_lock(&jobs->mutex);

	temp = (Job_node *)malloc(sizeof(Job_node));
	memset(temp,0,sizeof(Job_node));
	assert(temp != NULL);

	temp->job = (Job *)malloc(sizeof(Job));
	memset(temp->job,0,sizeof(Job));
	assert(temp->job != NULL);
	
	temp->job->cmd = (char *)malloc((strlen(cmd) + 1) * sizeof(char));
	strcpy(temp->job->cmd,cmd);

	temp->job->label = (char *)malloc((strlen(label) + 1) * sizeof(char));
	strcpy(temp->job->label,label);

	job_id++;
	num_threads++;

	temp->job->interval = interval;
	temp->job->run_it = 1;
	temp->job->job_id = job_id;

	rc = pthread_create(&temp->job->thread, NULL, run, (void *)temp->job);

	//pthread_detach(temp->job->thread);

	if (jobs->head == NULL) {     /* list is empty */
		jobs->head = jobs->tail = temp;
		pthread_mutex_unlock(&jobs->mutex);
		return;
	} else { // list is not empty
		jobs->tail->next = temp;
		jobs->tail = temp;
		pthread_mutex_unlock(&jobs->mutex);
		return;
	}
}

int kill_process(pid_t pid){
	int iter = 10;
	int start = 0;
	killpg(pid, SIGINT);
	//killpg(pid, SIGTERM);

	//killpg(pid, SIGKILL);
	/*
	while((kill(pid, 0)) == 0) {
		dbg_printf("Sleeping waiting for process %d to exit....\n", pid);
		u_sleep(100000);
		start++;
		if(start > iter){
			dbg_printf("Killing %d with SIGKILL....\n", pid);
			killpg(pid, SIGKILL);
		}
	}
	*/
}

int delete_job(Job_list *jobs, char *label, int job_id) {

	Job_node *iter,*previous;
	previous = NULL;
	iter = jobs->head;
	int found = 0;

	if(iter == NULL)
		return(0);

	while(iter->next != NULL){
		if((label != NULL && (strcmp(iter->job->label,label)) == 0) || (job_id != 0 && iter->job->job_id == job_id)) {
			found = 1;

			pthread_mutex_lock(&jobs->mutex);

			if(previous == NULL) { /* First item */
				jobs->head = iter->next;
				break;
			}else {
				previous->next = iter->next;	
				break;
			}
		}
		previous = iter;
		iter = iter->next;
	}

	if(found == 0) {
		if((label != NULL && (strcmp(iter->job->label,label)) == 0) || (job_id != 0 && iter->job->job_id == job_id)) {
			pthread_mutex_lock(&jobs->mutex);
			if(jobs->tail == jobs->head){
				jobs->tail = NULL;
				jobs->head = NULL;
			}else{
				jobs->tail = previous;
				previous->next = NULL;
			}
			found = 1;
		}
	}
	if(found > 0){
		
		pthread_cancel(iter->job->thread);

		pthread_join(iter->job->thread, NULL);

		kill_process(iter->job->pid);

		free(iter->job->label);
		free(iter->job->cmd);
		free(iter->job);
		free(iter);
		iter = NULL;
		num_threads--;
		pthread_mutex_unlock(&jobs->mutex);
	}
	return(found);
}


/*int exec_shell(const char *cmd) {
	FILE *p = NULL;
	if ((p = popen(cmd, "w")) == NULL)
		return (-1);

	return (pclose(p));
}
*/


int u_sleep(long usec) {
	struct timeval tv;
	tv.tv_sec = usec/1000000L;
	tv.tv_usec = usec%1000000L;
	return select(0, 0, 0, 0, &tv);
}


void print_status(Job_list *jobs, int connection_fd){
	Job_node *iter = jobs->head;
	int nbytes;
	char buffer[BUFSIZE];

	nbytes = snprintf(buffer, BUFSIZE,"=num_threads=%d\n", num_threads);
	write(connection_fd, buffer, nbytes);

	if(iter == NULL)
		return;

	while(iter->next != NULL){
		nbytes = snprintf(buffer, BUFSIZE,"=status[%d][cmd]=%s\n", iter->job->job_id, iter->job->cmd);
		write(connection_fd, buffer, nbytes);
		nbytes = snprintf(buffer, BUFSIZE,"=status[%d][interval]=%d\n", iter->job->job_id, iter->job->interval);
		write(connection_fd, buffer, nbytes);
		nbytes = snprintf(buffer, BUFSIZE,"=status[%d][label]=%s\n", iter->job->job_id, iter->job->label);
		write(connection_fd, buffer, nbytes);
		iter = iter->next;
	}
	nbytes = snprintf(buffer, BUFSIZE,"=status[%d][cmd]=%s\n", iter->job->job_id, iter->job->cmd);
	write(connection_fd, buffer, nbytes);
	nbytes = snprintf(buffer, BUFSIZE,"=status[%d][interval]=%d\n", iter->job->job_id, iter->job->interval);
	write(connection_fd, buffer, nbytes);
	nbytes = snprintf(buffer, BUFSIZE,"=status[%d][label]=%s\n", iter->job->job_id, iter->job->label);
	write(connection_fd, buffer, nbytes);
}

Job_node *locate_job(Job_list *jobs, char *label){
	Job_node *iter = jobs->head;

	if(iter == NULL)
		return NULL;
	
	while(iter->next != NULL){
		if(iter->job != NULL && iter->job->label != NULL){
			if((strcmp(iter->job->label, label)) == 0){
				return iter;
			}
		}
		iter = iter->next;
	}
	if((strcmp(iter->job->label, label)) == 0){
		return iter;
	}

	return NULL;
}

int connection_handler(Job_list *jobs, int connection_fd) {
	int nbytes;
	char buffer[BUFSIZE];
	nbytes = read(connection_fd, buffer, BUFSIZE);
	buffer[nbytes] = 0;
	char op[BUFSIZE];
	char command[BUFSIZE];
	int interval,job_id;
	int thread_wait = 0;
	char label[BUFSIZE];
	
	if(sscanf(buffer, "op=job intvl=%d lbl=%s cmd=%[^\t\n] %*s",  &interval, label, command)) {
		dbg_printf("Processing new job request.\n");
		if((locate_job(jobs, label)) == NULL){
			add_job(jobs, label, command, interval);
			nbytes = snprintf(buffer, BUFSIZE,"+%s_repaired\n", label);
			write(connection_fd, buffer, nbytes);
		}else{
			nbytes = snprintf(buffer, BUFSIZE,"+%s_exists\n", label);
			write(connection_fd, buffer, nbytes);
		}
	}else if(sscanf(buffer, "op=status%*s")) {
		dbg_printf("Returning status information.\n");
		print_status(jobs, connection_fd);
	} else if((strstr(buffer, "op=term")) != NULL) { 
		int lab = 0;
		int id = 0;
		int found = 0;
		if(sscanf(buffer, "op=term job_id=%d", &job_id)){
			dbg_printf("Request for terminating job with job id %d\n", job_id);	
			found = delete_job(jobs, NULL, job_id);
			id = 1;
		}else if(sscanf(buffer, "op=term lbl=%s", label)){
			dbg_printf("Request for terminating job with label %s\n", label);	
			found = delete_job(jobs, label, 0);
			lab = 1;
		}
		if(found > 0){
			if(lab > 0){
				nbytes = snprintf(buffer, BUFSIZE,"+%s_label_terminated\n", label);
			}else if(id > 0){
				nbytes = snprintf(buffer, BUFSIZE, "+%d_id_terminated\n", job_id);
			}
		}else{
			if(lab > 0){
				nbytes = snprintf(buffer, BUFSIZE, "+%s_label_notfound\n", label);
			}else if(id > 0){
				nbytes = snprintf(buffer, BUFSIZE, "+%d_id_notfound\n", job_id);
			}
		}
		if(id > 0 || lab > 0)
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
		printf("socket() failed\n");
		return 1;
	}

	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, UNIX_PATH_MAX, SOCKPATH);

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0) {
		printf("connect() failed\n");
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

static void signal_handler(int sig){
	
	if (sig == SIGINT || sig == SIGTERM) {
		Job_list *jobs = (Job_list *) job_l;
		Job_node *iter = jobs->head;		
		Job_node *previous = NULL;		
		if(iter != NULL) {
			while(iter->next != NULL){
				if(previous != NULL) {
					free(previous->job->label);
					free(previous->job->cmd);
					free(previous->job);
					free(previous);
				}
				pthread_cancel(iter->job->thread);
				kill_process(iter->job->pid);
				previous = iter;
				iter = iter->next;
			}
			pthread_cancel(iter->job->thread);
			if(previous != NULL) {
				free(previous->job->label);
				free(previous->job->cmd);
				free(previous->job);
				free(previous);
			}
			if(iter != NULL){
				kill_process(iter->job->pid);
				free(iter->job->label);
				free(iter->job->cmd);
				free(iter->job);
				free(iter);
			}
		}
		dbg_printf("Caught signal %d. Exiting gracefully....\n", sig);
		close(socket_fd);
		unlink(SOCKPATH);
		exit(0);
	}
}

void init_signals(void){
	sigact.sa_handler = signal_handler;
	sigemptyset(&sigact.sa_mask);
	sigact.sa_flags = 0;
	sigaction(SIGINT, &sigact, (struct sigaction *)NULL);
	sigaction(SIGTERM, &sigact, (struct sigaction *)NULL);
}

void initialize_p (void *p) {
     job_l = p;
}

int main(int argc, char *argv[]) {
	Job_list *jobs = (Job_list *)malloc(sizeof(Job_list));
	jobs->head = NULL;
	jobs->tail = NULL;
	//memset(jobs->tail,0,sizeof(Job_node));

	initialize_p(jobs);

/*
add_job(jobs,"label5","sleep 3 && date >> /tmp/test1.txt",5);
add_job(jobs,"label6","sleep 3 && date >> /tmp/test1.txt",5);
delete_job(jobs,"label5",0);
print_status(jobs,0);

exit(0);
*/

/*	add_job(jobs,"label2","command2",5);
	add_job(jobs,"label3","command3",5);
	add_job(jobs,"label4","command4",5);
	add_job(jobs,"label5","command5",5);

	delete_job(jobs,"label5",0);

	add_job(jobs,"label9","command9",5);
	add_job(jobs,"label6","command6",5);
	add_job(jobs,"label7","command7",5);
	add_job(jobs,"label8","command8",5);
*/


/*	print_jobs(jobs); */

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
	init_signals();

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
	} else {
		dbg_printf("Starting in foreground mode as debug was specified\n");
	}

	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0) {
		printf("socket() failed\n");
		return 1;
	} 

	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, UNIX_PATH_MAX, SOCKPATH);

	if(bind(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0) {
		printf("bind() failed\n");
		return 1;
	}

	if(listen(socket_fd, 5) != 0) {
		printf("listen() failed\n");
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
		dbg_printf("Handling new connection.\n");
		connection_handler(jobs, connection_fd);
	}

	close(socket_fd);
	unlink(SOCKPATH);
	return(0);
}
