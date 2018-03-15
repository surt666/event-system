(defproject event-system "0.12.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.amazonaws/aws-lambda-java-core "1.1.0"]
                 [expectations "2.1.9"]
                 [cheshire "5.6.3"]
                 [danlentz/clj-uuid "0.1.7"]
                 [clj-time "0.14.2"]
                 [amazonica "0.3.113" :exclusions [com.amazonaws/aws-java-sdk-devicefarm
                                                  com.amazonaws/aws-java-sdk-xray
                                                  com.amazonaws/aws-java-sdk-simpledb
                                                  com.amazonaws/aws-java-sdk-opsworkscm
                                                  com.amazonaws/aws-java-sdk-servermigration
                                                  com.amazonaws/aws-java-sdk-servicecatalog
                                                  com.amazonaws/aws-java-sdk-simpleworkflow
                                                  com.amazonaws/aws-java-sdk-route53
                                                  com.amazonaws/aws-java-sdk-importexport
                                                  com.amazonaws/aws-java-sdk-rds
                                                  com.amazonaws/aws-java-sdk-glacier
                                                  com.amazonaws/aws-java-sdk-redshift
                                                  com.amazonaws/aws-java-sdk-elasticbeanstalk
                                                  com.amazonaws/aws-java-sdk-elasticloadbalancing
                                                  com.amazonaws/aws-java-sdk-elasticloadbalancingv2
                                                  com.amazonaws/aws-java-sdk-emr
                                                  com.amazonaws/aws-java-sdk-elastictranscoder
                                                 ; com.amazonaws/aws-java-sdk-dynamodb
                                                  com.amazonaws/aws-java-sdk-ec2
                                                  com.amazonaws/aws-java-sdk-budgets
                                                  com.amazonaws/aws-java-sdk-directconnect
                                                  com.amazonaws/aws-java-sdk-ses
                                                  com.amazonaws/aws-java-sdk-autoscaling
                                                  com.amazonaws/aws-java-sdk-opsworks
                                                  com.amazonaws/aws-java-sdk-sts
                                                  com.amazonaws/aws-java-sdk-sqs
                                                  com.amazonaws/aws-java-sdk-elasticache
                                                  ;com.amazonaws/aws-java-sdk-sns
                                                  com.amazonaws/aws-java-sdk-cloudwatchmetrics
                                                  com.amazonaws/aws-java-sdk-codedeploy
                                                  com.amazonaws/aws-java-sdk-codepipeline
                                                  com.amazonaws/aws-java-sdk-storagegateway
                                                  com.amazonaws/aws-java-sdk-ecs
                                                  com.amazonaws/aws-java-sdk-ecr
                                                  com.amazonaws/aws-java-sdk-cloudhsm
                                                  com.amazonaws/aws-java-sdk-ssm
                                                  com.amazonaws/aws-java-sdk-workspaces
                                                  com.amazonaws/aws-java-sdk-machinelearning
                                                  com.amazonaws/aws-java-sdk-directory
                                                  com.amazonaws/aws-java-sdk-efs
                                                  com.amazonaws/aws-java-sdk-codecommit
                                                  com.amazonaws/aws-java-sdk-elasticsearch
                                                  com.amazonaws/aws-java-sdk-marketplacecommerceanalytics
                                                  com.amazonaws/aws-java-sdk-waf
                                                  com.amazonaws/aws-java-sdk-inspector
                                                  com.amazonaws/aws-java-sdk-iot
                                                  com.amazonaws/aws-java-sdk-api-gateway
                                                  com.amazonaws/aws-java-sdk-acm
                                                  com.amazonaws/aws-java-sdk-gamelift
                                                  com.amazonaws/aws-java-sdk-dms
                                                  com.amazonaws/aws-java-sdk-marketplacemeteringservice
                                                  com.amazonaws/aws-java-sdk-cognitoidp
                                                  com.amazonaws/aws-java-sdk-discovery
                                                  com.amazonaws/aws-java-sdk-rekognition
                                                  com.amazonaws/aws-java-sdk-applicationautoscaling
                                                  com.amazonaws/aws-java-sdk-snowball
                                                  com.amazonaws/aws-java-sdk-polly
                                                  com.amazonaws/aws-java-sdk-lightsail
                                                  com.amazonaws/aws-java-sdk-stepfunctions
                                                  com.amazonaws/aws-java-sdk-health
                                                  com.amazonaws/aws-java-sdk-codebuild
                                                  com.amazonaws/aws-java-sdk-appstream
                                                  com.amazonaws/aws-java-sdk-shield
                                                  com.amazonaws/aws-java-sdk-cloudtrail
                                                  com.amazonaws/aws-java-sdk-kinesis
                                                  com.amazonaws/aws-java-sdk-cognito
                                                  com.amazonaws/aws-java-sdk-cloudwatch
                                                  ; com.amazonaws/aws-java-sdk-cloudsearch
                                                  com.amazonaws/aws-java-sdk-cloudfront
                                                  com.amazonaws/aws-java-sdk-cloudformation
                                                  com.amazonaws/aws-java-sdk-pinpoint
                                                  com.amazonaws/aws-java-sdk-cognitoidentity
                                                  com.amazonaws/aws-java-sdk-lambda
                                                  com.amazonaws/aws-java-sdk-kms
                                                  com.amazonaws/aws-java-sdk-config
                                                  com.amazonaws/aws-java-sdk-model
                                                  com.amazonaws/aws-java-sdk-datapipeline
                                                  ]
                  ]]
  :plugins [[lein-expectations "0.0.7"]]
  :uberjar-exclusions [#".*-model\.json" #".*-intermediate\.json"]
  :aot :all)
