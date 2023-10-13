@Library('com.optum.jenkins.pipeline.library@master') _
def explorerProjectName = params.Explorer_Project_Name+"-"+params.Environment
currentBuild.displayName = explorerProjectName+"-"+currentBuild.number
def terraformbuild() {
                //command 'terraform --version'
                //sh """. /etc/profile.d/jenkins.sh
                //terraform -version"""
                //sh '/tools/terraform/terraform-0.12.18/terraform --version'
                script {
                        dir('terraform'){
                                    withVaultSecrets( credentialsId: "VAULT_APPROLE_SECRET_EXPLORER", //VAULT_APPROLE_SECRET is added to the Jenkins instance.
                                        namespace: 'OPTUM/APP/ORX-IDW/PROD',
                                        secretPath: '/KV2/Explorer/MPCReporting_VaultSecret_Prod',
                                        keys:  ['ARM_TENANT_ID',
                                                'ARM_SUBSCRIPTION_ID',
                                                'ARM_CLIENT_ID',
                                                'ARM_CLIENT_SECRET',
                                                'TF_VAR_TERRAFORM_RG',
                                                'TF_VAR_TERRAFORM_SA',
                                                'TF_VAR_SYNAPSE_USER',
                                                'TF_VAR_SYNAPSE_PASS',
                                                'TF_VAR_SYNAPSE_ETLUSER',
                                                'TF_VAR_SYNAPSE_ETLPASS'] ) {
                                            sh '''
                                                env | egrep -i "TF_VAR_|ARM_" > ~/secrets.sh
                                                sed -i 's/^/export /g' ~/secrets.sh
                                                chmod 700 ~/secrets.sh
                                                set +x              # Stop command echo
                                                . /etc/profile.d/jenkins.sh
                                                . ~/secrets.sh
                                                az login --service-principal -u $ARM_CLIENT_ID -p $ARM_CLIENT_SECRET -t $ARM_TENANT_ID
                                                az account set --subscription $ARM_SUBSCRIPTION_ID
                                                export ACCOUNT_KEY=$(az storage account keys list --resource-group ${TF_VAR_TERRAFORM_RG} --account-name ${TF_VAR_TERRAFORM_SA} --query '[0].value' -o tsv)
                                                set -x              # Start command echo
                                                export ARM_CLIENT_ID=$ARM_CLIENT_ID ARM_CLIENT_SECRET=$ARM_CLIENT_SECRET ARM_TENANT_ID=$ARM_TENANT_ID ARM_SUBSCRIPTION_ID=$ARM_SUBSCRIPTION_ID
                                                terraform init -var-file="env/${TFENV}/global.tfvars" -backend-config="env/${TFENV}/backend.tfvars"
                                                terraform plan -var-file="env/${TFENV}/global.tfvars" -var-file="env/${TFENV}/backend.tfvars" -var="clientSecret=${ARM_CLIENT_SECRET}" -var="admin_username=${TF_VAR_SYNAPSE_USER}" -var="admin_password=${TF_VAR_SYNAPSE_PASS}" -var="sql_username=${TF_VAR_SYNAPSE_ETLUSER}" -var="sql_password=${TF_VAR_SYNAPSE_ETLPASS}" -out=${TFAPP}-${TFENV}-tfplan
                                                terraform apply -auto-approve -no-color ${TFAPP}-${TFENV}-tfplan
                                                '''
                                        }
                            }
                }
}
pipeline() {
    agent {
    label 'docker-maven-slave'
    }
    environment {
        EMAIL_RECIPIENTS = 'subramanian_anantha@optum.com;'
        //-var="clientID=${ARM_CLIENT_ID}" -var="subscriptionID=${ARM_SUBSCRIPTION_ID}" -var="tenantID=${ARM_TENANT_ID}"
        // terraform apply -auto-approve -no-color ${TFAPP}-${TFENV}-tfplan
    }
    stages {
        stage('Terraform Infra Build'){
            environment {
                TFENV = 'prod'
                TFAPP = 'mpcreporting'
            }
            steps {
                  terraformbuild()
            }
        }
    }
post {
    always {
      echo 'This will always run'
    }
    success {
      echo 'This will run only if successful'
      echo "${BUILD_ID} - ${JOB_NAME}"
      emailext body: "Attention:\n The build #${BUILD_ID} for ${JOB_NAME} pipeline was SUCCESSFUL.\n Below are the details:\n \tBuild Tag: ${BUILD_TAG}\n \tBuild URL: ${BUILD_URL}",
        subject: "$currentBuild.currentResult-$JOB_NAME",
         to: "${EMAIL_RECIPIENTS}"
    }
    failure {
      echo 'This will run only if failed'
            emailext body: "Attention:\n The build #${BUILD_ID} for ${JOB_NAME} pipeline was FAILED.\n Below are the details:\n \tBuild Tag: ${BUILD_TAG}\n \tBuild URL: ${BUILD_URL}",
        subject: "$currentBuild.currentResult-$JOB_NAME",
         to: "${EMAIL_RECIPIENTS}"
    }
    unstable {
      echo 'This will run only if the run was marked as unstable'
    }
    changed {
      echo 'This will run only if the state of the Pipeline has changed'
      echo 'For example, if the Pipeline was previously failing but is now successful'
    }
  }
}  //Pipeline end