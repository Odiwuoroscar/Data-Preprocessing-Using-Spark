# Data-Preprocessing-Using-Spark
The primary goal for this project is to use Apache Spark for distributed data preprocessing to maximize resource utilization and minimize data processing time.

The pytho script used starts by importing the necessary libraries Python program for data preprocessing then it goes a head to handle missing values through imputation. Numerical missing values were filled using mean imputation, while categorical gaps were addressed with mode imputation. The data is then scrutinized the dataset for duplicate entries, removing any found to maintain data integrity. For the exploratory phase, the project employed various visualization techniques. Histograms were created to illustrate the distribution patterns of numerical predictors, while bar plots were utilized to showcase the frequency distribution of categorical variables. To identify potential outliers in numerical features, I generated boxplots.
The final step involved creating a correlation matrix. This allowed me to examine the relationships between various predictors and the response variable (exam score). This matrix proved invaluable in highlighting strongly correlated predictors, which could be crucial for subsequent analysis stages.

I will be usign to VMs in this project, so I configured my two VMs and installed and configured Apache spark.

First let me generate ssh key pair for connection between the two vms (master and worker) I generated ssh key pair in hadoop1 then copied the key to the other vm (hadoop 2)

Ssh-keygen -t rsa

cat.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

Then I configure Spark in the cluster mode.

I user tar spark package on hadoop1 and scp to hadoop2

On hadoop1

cd /opt

tar czf spark.tar.gz spark

scp spark.tar.gz root@hadoop2:/opt

On hadoop2

cd /opt

tar xvzf spark.tar.gz

To start spark

/opt/spark/sbin/start-all.sh

To submit a spark job I used Command

-- /opt/spark/bin/spark-submit --master spark://hadoop1:7077 /opt/spark_scrypt.py

Performance of the program

I observed that the duration for completing the action using only 1 Vm was approximately 2.0 minutes. When I ran the command again to check for consistency, the time remained almost the same at 2.0 minutes. This indicates that running the Python code on a single VM (using Hadoop 1 only) consistently takes about 2.4 minutes.

The duration for completing a specific action using two VMs was about 8 seconds. This is approximately five times faster than the time taken when using only one VM, which was around 2.4 minutes. Running the Python code using both VMs (Hadoop 1 and Hadoop 2) demonstrates a significant improvement in processing speed.
