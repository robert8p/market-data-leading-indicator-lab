#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <limits.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/msg.h>
#include <linux/audit.h>
#include <linux/capability.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sched.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

/* Stable Linux uapi layout. ABI 9 introduced pathname-socket resolution. */
struct rules { uint64_t fs, net, scopes; };
struct __attribute__((packed)) beneath { uint64_t access; int32_t fd; };
#define RFILE (1ULL << 2)
#define RDIR (1ULL << 3)
#define EXEC (1ULL << 0)
#define WFILE (1ULL << 1)
#define ALLFS ((1ULL << 17) - 1)
#define RESOLVE_UNIX (1ULL << 16)
#define FS_FILE_BITS (RFILE|WFILE|EXEC|(1ULL<<14)|(1ULL<<15))
#define RUNTIME (RFILE|RDIR|EXEC)
#define LL_VERSION 1
#define PATH_BENEATH 1
#define SCOPE_UNIX_AND_SIGNAL 3
#if defined(__x86_64__)
#define NATIVE_ARCH AUDIT_ARCH_X86_64
#define MULTI "x86_64-linux-gnu"
#elif defined(__aarch64__)
#define NATIVE_ARCH AUDIT_ARCH_AARCH64
#define MULTI "aarch64-linux-gnu"
#else
#error Unsupported fixed-isolation architecture
#endif

static void fail(const char *stage){ fprintf(stderr,"EQ17_GUARD_BLOCK %s errno=%d\n",stage,errno);_exit(77); }
static void need(int ok,const char *s){ if(!ok)fail(s); }
static void path(char *out,const char *a,const char *b){ int n=snprintf(out,PATH_MAX,"%s/%s",a,b);need(n>0&&n<PATH_MAX,"path_length"); }
static void check_dir(const char *name, int writable){
    char real[PATH_MAX];struct stat s;
    need(realpath(name,real)!=NULL&&!strcmp(real,name)&&lstat(name,&s)==0&&S_ISDIR(s.st_mode),"capsule_directory");
    need(s.st_uid==geteuid()&&!(s.st_mode&0022),"capsule_owner_mode");
    if(writable)need((s.st_mode&0700)==0700,"work_owner_mode");
}
static void allow(int rs,const char *name,uint64_t access,int required){
    int fd=open(name,O_PATH|O_CLOEXEC);
    if(fd<0){if(!required&&errno==ENOENT)return;fail("runtime_allow_path");}
    struct stat s;need(fstat(fd,&s)==0,"allow_stat");
    struct beneath r={S_ISDIR(s.st_mode)?access:(access&FS_FILE_BITS),fd};
    need(syscall(SYS_landlock_add_rule,rs,PATH_BENEATH,&r,0)==0,"landlock_path_rule");close(fd);
}
static void fixed_glob(int rs,const char *pattern,uint64_t rights){
    glob_t g={0};int r=glob(pattern,GLOB_NOSORT,NULL,&g);
    if(r!=GLOB_NOMATCH){need(r==0,"runtime_pattern");for(size_t i=0;i<g.gl_pathc;i++)allow(rs,g.gl_pathv[i],rights,1);}
    globfree(&g);
}
static void ll(const char *root,int abi){
    uint64_t fs=(abi>=9)?ALLFS:((abi>=5)?((1ULL<<16)-1):((1ULL<<15)-1));
    struct rules r={fs,0,(abi>=6)?SCOPE_UNIX_AND_SIGNAL:0};
    size_t bytes=(abi>=6)?24:((abi>=4)?16:8);
    int rs=(int)syscall(SYS_landlock_create_ruleset,&r,bytes,0);need(rs>=0,"landlock_create");
    char p[PATH_MAX];
    const char *read_dirs[]={"packet","candidate"};
    for(unsigned i=0;i<2;i++){path(p,root,read_dirs[i]);allow(rs,p,RFILE|RDIR,1);}
    path(p,root,"work");allow(rs,p,fs&~EXEC,1);
    path(p,root,"result");allow(rs,p,fs&~EXEC&~RESOLVE_UNIX,1);
    const char *libs[]={"/usr/lib/"MULTI,"/usr/lib/locale","/usr/share/zoneinfo","/usr/share/locale",
        "/usr/share/postgresql","/usr/local/lib/python3.12","/usr/lib/postgresql"};
    for(size_t i=0;i<sizeof(libs)/sizeof(libs[0]);i++)allow(rs,libs[i],RUNTIME,0);
    fixed_glob(rs,"/usr/local/lib/libpython3.12.so*",RFILE|EXEC);
    const char *exe[]={"/usr/local/bin/python3.12","/usr/sbin/pgbouncer","/bin/sh"};
    for(unsigned i=0;i<3;i++)allow(rs,exe[i],RFILE|EXEC,1);
    const char *read[]={"/etc/passwd","/etc/group","/etc/nsswitch.conf","/etc/ld.so.cache","/etc/localtime",
        "/etc/os-release","/dev/urandom","/dev/random","/dev/zero","/sys/fs/cgroup/cpu.max","/sys/fs/cgroup/memory.max"};
    for(size_t i=0;i<sizeof(read)/sizeof(read[0]);i++)allow(rs,read[i],RFILE,0);
    allow(rs,"/dev/null",RFILE|WFILE,1);
    need(syscall(SYS_landlock_restrict_self,rs,0)==0,"landlock_restrict");close(rs);
}
#define LOAD(off) BPF_STMT(BPF_LD|BPF_W|BPF_ABS,(off))
#define CMP(v,t,f) BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,(v),(t),(f))
#define RET(v) BPF_STMT(BPF_RET|BPF_K,(v))
#define DENY (SECCOMP_RET_ERRNO|EPERM)
#define BLOCK(n) CMP(__NR_##n,0,1),RET(DENY)
#define SOCK(n,dom) CMP(__NR_##n,0,3),LOAD(offsetof(struct seccomp_data,args[0])),CMP(dom,1,0),RET(DENY),LOAD(offsetof(struct seccomp_data,nr))
static void sec(int mode){
    struct sock_filter ins[]={
        LOAD(offsetof(struct seccomp_data,arch)),CMP(NATIVE_ARCH,1,0),RET(SECCOMP_RET_KILL_PROCESS),
        LOAD(offsetof(struct seccomp_data,nr)),
        BPF_JUMP(BPF_JMP|BPF_JSET|BPF_K,0x40000000,0,1),RET(DENY),
        SOCK(socket,(uint32_t)(mode?AF_INET:AF_UNIX)),SOCK(socketpair,AF_UNIX),
        BLOCK(ptrace),BLOCK(process_vm_readv),BLOCK(process_vm_writev),BLOCK(pidfd_getfd),BLOCK(kcmp),
        BLOCK(io_uring_setup),BLOCK(io_uring_enter),BLOCK(io_uring_register),BLOCK(bpf),BLOCK(perf_event_open),
        BLOCK(userfaultfd),BLOCK(open_by_handle_at),BLOCK(name_to_handle_at),BLOCK(unshare),BLOCK(setns),
        BLOCK(mount),BLOCK(umount2),BLOCK(chroot),BLOCK(pivot_root),BLOCK(open_tree),BLOCK(move_mount),
        BLOCK(mount_setattr),BLOCK(fsopen),BLOCK(fsmount),BLOCK(fsconfig),BLOCK(fspick),
        BLOCK(keyctl),BLOCK(add_key),BLOCK(request_key),BLOCK(msgget),BLOCK(msgsnd),BLOCK(msgrcv),BLOCK(msgctl),
        CMP(__NR_clone3,0,1),RET(SECCOMP_RET_ERRNO|ENOSYS),
        CMP(__NR_clone,0,3),LOAD(offsetof(struct seccomp_data,args[0])),
        BPF_JUMP(BPF_JMP|BPF_JSET|BPF_K,CLONE_NEWUSER|CLONE_NEWNET|CLONE_NEWIPC|CLONE_NEWPID|CLONE_NEWNS|CLONE_NEWUTS|CLONE_NEWCGROUP|CLONE_PTRACE,0,1),RET(DENY),
        RET(SECCOMP_RET_ALLOW)
    };
    struct sock_fprog p={(unsigned short)(sizeof(ins)/sizeof(ins[0])),ins};
    need(prctl(PR_SET_SECCOMP,SECCOMP_MODE_FILTER,&p)==0,"seccomp_install");
}
static int unix_listener(struct sockaddr_un *addr,socklen_t n){
    int s=socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0);need(s>=0,"sentinel_socket");
    need(bind(s,(struct sockaddr *)addr,n)==0&&listen(s,1)==0,"sentinel_listen");return s;
}
static void denied_endpoint(struct sockaddr_un *a,socklen_t len){
    int s=socket(AF_UNIX,SOCK_STREAM,0);need(s>=0,"positive_unix_socket");
    int rc=connect(s,(struct sockaddr *)a,len);
    need(rc<0&&(errno==EACCES||errno==EPERM),"outside_unix_not_denied");close(s);
}
static void put(const char *name,const char *value,const char *open_reason){
    int fd=open(name,O_WRONLY|O_CLOEXEC);need(fd>=0,open_reason);
    size_t n=strlen(value);need(write(fd,value,n)==(ssize_t)n,"namespace_map_write");close(fd);
}
static ino_t ns_id(const char *s){
    char p[PATH_MAX];struct stat st;path(p,"/proc/self/ns",s);need(stat(p,&st)==0,"namespace_stat");return st.st_ino;
}
static void zero_caps(void){
    struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct d[2]={{0},{0}};
    need(syscall(SYS_capset,&h,d)==0,"namespace_drop_caps");
    need(syscall(SYS_capget,&h,d)==0,"namespace_read_caps");
    for(int i=0;i<2;i++)need(!(d[i].effective|d[i].permitted|d[i].inheritable),"namespace_cap_drop_failed");
}
static void empty_sysv(void){
    struct shm_info si={0};struct seminfo se={0};struct msginfo mi={0};
    need(shmctl(0,SHM_INFO,(struct shmid_ds *)(void *)&si)>=0&&si.used_ids==0,"nonempty_or_unproven_shm");
    need(semctl(0,0,SEM_INFO,&se)>=0&&se.semusz==0,"nonempty_or_unproven_sem");
    need(msgctl(0,MSG_INFO,(struct msqid_ds *)(void *)&mi)>=0&&mi.msgpool==0,"nonempty_or_unproven_msg");
}
static void private_namespaces(void){
    uid_t u=getuid();gid_t g=getgid();char map[96];
    ino_t net=ns_id("net"),ipc=ns_id("ipc"),user=ns_id("user"),pid_for=ns_id("pid");
    /* This mapping helper has a clean environment and no credential descriptors.
       Leave the credentialed parent permanently non-dumpable. Kernel procfs uses
       dumpability to assign ownership of the helper's own uid/gid map files. */
    need(prctl(PR_SET_DUMPABLE,1)==0,"mapping_helper_dumpability");
    need(unshare(CLONE_NEWUSER|CLONE_NEWNET|CLONE_NEWIPC|CLONE_NEWPID)==0,"private_namespaces_unavailable");
    snprintf(map,sizeof(map),"%lu %lu 1\n",(unsigned long)u,(unsigned long)u);put("/proc/self/uid_map",map,"uid_map_open");
    put("/proc/self/setgroups","deny","setgroups_map_open");
    snprintf(map,sizeof(map),"%lu %lu 1\n",(unsigned long)g,(unsigned long)g);put("/proc/self/gid_map",map,"gid_map_open");
    need(prctl(PR_SET_DUMPABLE,0)==0,"mapping_helper_reseal");
    need(getuid()==u&&geteuid()==u&&getgid()==g&&getegid()==g,"uid_map_mismatch");
    need(ns_id("net")!=net&&ns_id("ipc")!=ipc&&ns_id("user")!=user,"unchanged_namespace");
    struct if_nameindex *list=if_nameindex();need(list!=NULL,"list_private_interfaces");
    int count=0;for(struct if_nameindex *j=list;j->if_index;j++){count++;need(!strcmp(j->if_name,"lo"),"private_nonloopback_interface");}
    if_freenameindex(list);need(count==1,"private_loopback_only");
    struct ifreq ifr={0};strcpy(ifr.ifr_name,"lo");int fd=socket(AF_INET,SOCK_DGRAM|SOCK_CLOEXEC,0);
    need(fd>=0&&ioctl(fd,SIOCGIFFLAGS,&ifr)==0,"private_loopback_flags");
    ifr.ifr_flags|=IFF_UP;need(ioctl(fd,SIOCSIFFLAGS,&ifr)==0,"private_loopback_up");close(fd);
    pid_t child=fork();need(child>=0,"private_pid_fork");
    if(child>0){
        zero_caps();int status;while(waitpid(child,&status,0)<0)if(errno!=EINTR)fail("private_pid_wait");
        _exit(WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status));
    }
    need(getpid()==1&&ns_id("pid")!=pid_for,"private_pid_unproven");
    zero_caps();
}
static void sterile_environment(void){
    extern char **environ;
    const char *names[]={"PATH","LANG","LC_ALL","HOME","TMPDIR","PYTHONDONTWRITEBYTECODE",
        "PYTHONUNBUFFERED","MALLOC_ARENA_MAX","EQ17_T0_NS","EQ17_ARM_MS","EQ17_WORK_DIR",
        "EQ17_CANDIDATE_ROOT","EQ17_CANDIDATE_ENVELOPE","EQ17_RESULT_DIR"};
    for(char **v=environ;*v;v++){
        char *eq=strchr(*v,'=');int known=0;need(eq!=NULL,"invalid_environment");
        for(unsigned i=0;i<sizeof(names)/sizeof(names[0]);i++)
            if(strlen(names[i])==(size_t)(eq-*v)&&!strncmp(*v,names[i],(size_t)(eq-*v))){known=1;break;}
        need(known,"nonsterile_environment");
    }
}
int main(int argc,char **argv){
    need(argc==5&&!strcmp(argv[2],"/usr/local/bin/python")&&!strcmp(argv[3],"-B"),"fixed_argv");
    need(getuid()!=0&&getuid()==geteuid()&&getgid()==getegid(),"ordinary_uid");
    struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct cd[2];
    need(syscall(SYS_capget,&h,cd)==0,"capget");
    for(int i=0;i<2;i++)need(!(cd[i].effective|cd[i].permitted|cd[i].inheritable),"zero_capabilities_required");
    need(prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0)==0&&prctl(PR_SET_DUMPABLE,0)==0,"lock_privs");
    struct rlimit lim={0,0};need(setrlimit(RLIMIT_CORE,&lim)==0,"no_core");
    sterile_environment();
    char *root=argv[1];need(!strncmp(root,"/tmp/eq17-native-",17)&&!strchr(root+17,'/'),"capsule_location");check_dir(root,1);
    char p[PATH_MAX],execp[PATH_MAX],work[PATH_MAX];path(work,root,"work");
    const char *parts[]={"work","result","packet","candidate"};
    for(unsigned i=0;i<4;i++){path(p,root,parts[i]);check_dir(p,i<2);}
    path(execp,root,"packet/native_transport/run_fixture.py");need(!strcmp(execp,argv[4]),"fixed_script");
    for(int i=0;i<=2;i++){struct stat st;need(fstat(i,&st)==0&&(S_ISREG(st.st_mode)||S_ISFIFO(st.st_mode)||
        (S_ISCHR(st.st_mode)&&major(st.st_rdev)==1&&minor(st.st_rdev)==3)),"stdio_must_be_private_file_pipe_or_null");}
    need(syscall(SYS_close_range,3,~0U,0)==0,"close_inherited_descriptors");
    char sentinel[]="/tmp/eq17-isolation-check-XXXXXX";need(mkdtemp(sentinel)!=NULL,"sentinel_tmp");
    char file[PATH_MAX],sockpath[PATH_MAX];path(file,sentinel,"denied");path(sockpath,sentinel,"sock");
    int fd=open(file,O_CREAT|O_EXCL|O_WRONLY,0600);need(fd>=0,"sentinel_file");close(fd);
    struct sockaddr_un a={.sun_family=AF_UNIX},b={.sun_family=AF_UNIX};
    need(strlen(sockpath)<sizeof(a.sun_path),"sun_length");strcpy(a.sun_path,sockpath);
    snprintf(b.sun_path+1,sizeof(b.sun_path)-1,"eq17-restrict-%ld",(long)getpid());
    socklen_t alen=(socklen_t)(offsetof(struct sockaddr_un,sun_path)+strlen(a.sun_path)+1);
    socklen_t blen=(socklen_t)(offsetof(struct sockaddr_un,sun_path)+1+strlen(b.sun_path+1));
    int as=unix_listener(&a,alen),bs=unix_listener(&b,blen);
    int abi=(int)syscall(SYS_landlock_create_ruleset,NULL,0,LL_VERSION);
    if(abi>=1)dprintf(2,"EQ17_LANDLOCK_ABI %d\n",abi);
    need(abi>=3,"landlock_abi3_required");
    int mode=abi<9, tcp=-1;struct sockaddr_in local={.sin_family=AF_INET,.sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    if(mode){
        socklen_t size=sizeof(local);tcp=socket(AF_INET,SOCK_STREAM|SOCK_CLOEXEC,0);
        need(tcp>=0&&bind(tcp,(struct sockaddr *)&local,size)==0&&listen(tcp,1)==0&&getsockname(tcp,(struct sockaddr *)&local,&size)==0,"parent_tcp_sentinel");
    }
    pid_t pid=fork();need(pid>=0,"guard_fork");
    if(pid==0){
        close(as);close(bs);if(tcp>=0)close(tcp);
        if(mode)private_namespaces();
        empty_sysv();ll(root,abi);sec(mode);
        fd=open(file,O_RDONLY);need(fd<0&&(errno==EACCES||errno==EPERM),"outside_file_not_denied");
        fd=socket(AF_INET,SOCK_STREAM,0);
        if(mode){
            need(fd>=0&&connect(fd,(struct sockaddr *)&local,sizeof(local))<0&&errno==ECONNREFUSED,"outside_tcp_reachable");close(fd);
            fd=socket(AF_UNIX,SOCK_STREAM,0);need(fd<0&&errno==EPERM,"outside_unix_family_not_denied");
        }else need(fd<0&&errno==EPERM,"inet_not_denied");
        fd=socket(AF_INET6,SOCK_STREAM,0);need(fd<0&&errno==EPERM,"inet6_not_denied");
        if(!mode){denied_endpoint(&a,alen);denied_endpoint(&b,blen);}
        need(syscall(SYS_io_uring_setup,0,NULL)<0&&errno==EPERM,"uring_not_denied");
        need(syscall(SYS_pidfd_getfd,-1,0,0)<0&&errno==EPERM,"pidfd_not_denied");
        path(p,root,"packet");need(chdir(p)==0,"packet_cwd");
        need(setenv("EQ17_NATIVE_MODE",mode?"private_netns":"unix_abi9",1)==0,"mode_env");
        dprintf(2,"EQ17_GUARD_PASS mode=%s external_file_denied capabilities_zero empty_sysv\n",mode?"private_netns":"unix_abi9");
        execv(argv[2],argv+2);fail("python_exec");
    }
    int status=0;while(waitpid(pid,&status,0)<0){if(errno!=EINTR)fail("guard_wait");}
    close(as);close(bs);if(tcp>=0)close(tcp);unlink(sockpath);unlink(file);rmdir(sentinel);
    return WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);
}
