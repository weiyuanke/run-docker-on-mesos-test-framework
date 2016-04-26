#!/usr/bin/env python
#-*- coding:utf8
import os
import sys
import time
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TOTAL_TASKS = 5
TASK_CPUS = 1
TASK_MEM = 128

class TestScheduler(mesos.interface.Scheduler):
    def __init__(self):
        self.imageName = 'ubuntu'#docker image to run
        self.numInstances = 5;  #num of docker instanace
        self.pendingInstances = [];
        self.runningInstances = [];
        self.tasksLaunched = 0

    #框架注册的回调
    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    #scheduler接收到mesos的resource offer
    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
            #接收到的总的资源数量
            print "Received offer %s with cpus: %s and mem: %s" % (offer.id.value, offerCpus, offerMem)

            if self.tasksLaunched > 10:
                driver.stop()

            if len(self.runningInstances) + len(self.pendingInstances) < self.numInstances:
                tid = self.tasksLaunched;
                self.tasksLaunched += 1;
                
                self.pendingInstances.append(str(tid))
                #创建任务
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid

                dockerinfo = mesos_pb2.ContainerInfo.DockerInfo();
                dockerinfo.image = self.imageName;
                dockerinfo.network = 2 # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
                dockerinfo.force_pull_image = True

                containerinfo = mesos_pb2.ContainerInfo();
                containerinfo.type = 1 #mesos_pb2.ContainerInfo.Type.DOCKER
                # Let's create a volume
                # container.volumes, in mesos.proto, is a repeated element
                # For repeated elements, we use the method "add()" that returns an object that can be updated
                volume = containerinfo.volumes.add()
                volume.container_path = "/mnt/mesosexample" # Path in container
                volume.host_path = "/tmp/mesosexample" # Path on host
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                #volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                # Define the command line to execute in the Docker container
                command = mesos_pb2.CommandInfo()
                command.value = "sleep 30"
                task.command.MergeFrom(command) 
                # The MergeFrom allows to create an object then to use this object in an other one. 
                #Here we use the new CommandInfo object and specify to use this instance for the parameter task.command.
                # We also need to tell docker to do mapping with this port
                #docker_port = dockerinfo.port_mappings.add()
                #docker_port.host_port = 80
                #docker_port.container_port = 80
         
                # CPUs are repeated elements too
                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 1
                
                # Memory are repeated elements too
                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 128
                
                containerinfo.docker.MergeFrom(dockerinfo)
                task.container.MergeFrom(containerinfo)
                tasks.append(task)
                
            operation = mesos_pb2.Offer.Operation()
            #operation的type为LAUNCH\RESERVE\UNRESERVE\CREATE\DESTROY
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            # Accepts the given offers and performs a sequence of operations on those accepted offers.
            driver.acceptOffers([offer.id], [operation])

    #该函数是task 和scheduler通信的接口，per task
    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))
        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FINISHED or \
           update.state == mesos_pb2.TASK_ERROR or \
           update.state == mesos_pb2.TASK_FAILED:
           print "message: "+update.message
           try:
               self.pendingInstances.remove(update.task_id.value);
               self.runningInstances.remove(update.task_id.value);
           except Exception,e:
               pass
        if update.state == mesos_pb2.TASK_RUNNING:
            self.pendingInstances.remove(update.task_id.value)
            self.runningInstances.append(update.task_id.value);
        print 'Number of instances: pending='+str(len(self.pendingInstances))+", runningInstances="+str(len(self.runningInstances))
            
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    #FrameworkInfo描述了一个framework
    framework = mesos_pb2.FrameworkInfo()
    #必须有user和name，user表示executor、task以那个用户的身份运行,默认当前用户
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    #If set, framework pid, executor pids and status updates are
    #checkpointed to disk by the slaves. Checkpointing allows a
    #restarted slave to reconnect with old executors and recover
    #status updates, at the cost of disk I/O.
    framework.checkpoint = True

    driver = mesos.native.MesosSchedulerDriver(TestScheduler(),framework,sys.argv[1])
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    # Ensure that the driver process terminates.
    driver.stop();
    sys.exit(status)
