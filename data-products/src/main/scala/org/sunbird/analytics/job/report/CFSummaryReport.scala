package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.UserCacheSupport
import org.sunbird.analytics.exhaust.collection.UDFUtils

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}


object CFSummaryReport extends IJob with BaseReportsJob with UserCacheSupport {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val reportCols = Seq("userid","firstname", "lastname", "username", "email", "usertype", "cin", "fmpsid", "province", "designation", "training_group", "orgname", "num_courses_enrolled", "num_courses_started", "num_courses_completed", "course_metrics")
  private val collectionBatchDBSettings = Map("table" -> "batches", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.collection.keyspace"), "cluster" -> "LMSCluster")
  private val hierarchyStoreSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.hierarchy.keyspace"), "cluster" -> "LMSCluster")
  private val userCourseDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "ReportCluster");

  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = "cf_summary_report"

  implicit val className: String = "org.sunbird.analytics.job.report.CFSummaryReport"
  val jobName = "CFSummaryReport"

  // Case class for batch info to ensure proper schema
  case class BatchInfo(
                        batch_name: String,
                        cf_id: String,
                        cl_id: String,
                        start_date: String,
                        end_date: String,
                        enrolled_date: String,
                        cf_progress: String,
                        cl_progress: String,
                        courses: Seq[CourseDetail]
                      )

  case class CourseDetail(
                           courseid: String,
                           course_code: String,
                           course_name: String
                         )

  case class CourseMetrics(
                            courses_enrolled: Seq[BatchInfo],
                            courses_started: Seq[BatchInfo],
                            courses_completed: Seq[BatchInfo]
                          )

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(prepareReport(fetchData))
      val reportData = res._2
      saveToPostgres(reportData)
      reportData.unpersist()
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def getUserEnrolromentColumns(): Seq[String] = {
    Seq("userid", "activityid", "batchid", "activitytype", "completedon", "enrolled_date", "datetime", "enrolleddate", "progress", "status")
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  // $COVERAGE-ON$
  def getUserEnrollment(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    val cols = getUserEnrolromentColumns()
    val df = fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .withColumn("enrolleddate", col("enrolled_date"))

    df.select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
  }

  def getCourseBatchDF(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, collectionBatchDBSettings, cassandraUrl, new StructType())
      .select("activityid", "batchid", "name", "start_date", "end_date")
  }


  def prepareReport(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    import spark.implicits._

    val userEnrolmentDF = getUserEnrollment(fetchData)
    val userCachedDF = getUserCacheDF(fetchData)
    val courseBatchDF = getCourseBatchDF(fetchData)

    // Join enrolments with course batch (to get start & end dates and batch name)
    val userJoinedWithBatchDF = userEnrolmentDF.join(courseBatchDF, Seq("activityid", "batchid"), "left")

    // Register UDFs properly
    val convertDateUDF = udf(convertDateFn)
    val formatCompletionDateUDF = udf(formatCompletionDate _)

    // Apply date transformations
    val userJoinedWithBatchConvertedDF = userJoinedWithBatchDF
      .withColumn("start_date", convertDateUDF(col("start_date")))
      .withColumn("end_date", convertDateUDF(col("end_date")))

    // Create a struct containing activityid, batchid, batch name, dates, activitytype
    val userJoinedWithCourseStruct = userJoinedWithBatchConvertedDF
      .withColumn("activity_info", struct(
        col("activityid"),
        col("batchid"),
        col("name").as("batch_name"),
        col("activitytype"),
        col("start_date"),
        col("end_date"),
        coalesce(col("enrolled_date"), col("enrolleddate")).cast("string").as("enrolled_date"),
        col("userid")
      ))

    // Collect activity-level data grouped by user
    val userActivityAggDF = userJoinedWithCourseStruct.groupBy("userid").agg(
      collect_set(when(col("enrolleddate").isNotNull, col("activity_info"))).as("activities_enrolled")
    )

    // Join back to user info
    val userSummaryDF = userCachedDF.join(userActivityAggDF, Seq("userid"), "left")
    val decryptedSummary = decryptUserInfo(userSummaryDF)

    println("[DEBUG] ===== STEP 2: Collect All Activity IDs =====")
    // Step 1: Collect all unique activity IDs from the DataFrame
    val allActivityIds = decryptedSummary
      .select(explode(col("activities_enrolled")))
      .filter(col("col").isNotNull)
      .select(col("col.activityid").as("activityid"))
      .distinct()
      .rdd.map(r => Option(r.getAs[String](0)))
      .collect().toList.flatten.distinct

    println(s"[DEBUG] Found ${allActivityIds.size} unique activity IDs: ${allActivityIds.mkString(", ")}")

    println("[DEBUG] ===== STEP 3: Get Course Enrollment Details =====")
    // Step 2: Get course enrollment details with status from getCourseIdentifiers
    val courseEnrollmentsMap = allActivityIds.map { activityid =>
      println(s"[DEBUG] Processing activityid: $activityid")
      val coursesWithEnrollments = getCourseIdentifiers(activityid)
      activityid -> coursesWithEnrollments
    }.toMap

    // Log course enrollment details
    courseEnrollmentsMap.foreach { case (activityid, coursesWithEnrollments) =>
      if (coursesWithEnrollments.nonEmpty) {
        val uniqueCourses = coursesWithEnrollments.map(c => s"${c._1}(${c._2})").distinct
        val enrollmentsWithUsers = coursesWithEnrollments.filter(_._4.nonEmpty)
        val userCount = enrollmentsWithUsers.map(_._4).distinct.size
        println(s"[DEBUG] Activity $activityid: ${uniqueCourses.size} courses, ${enrollmentsWithUsers.size} enrollments, $userCount users")
        JobLogger.log(s"Found ${uniqueCourses.size} unique courses for activityid $activityid with ${enrollmentsWithUsers.size} enrollments from $userCount users",
          Option(Map("activityid" -> activityid, "course_count" -> uniqueCourses.size, "enrollment_count" -> enrollmentsWithUsers.size, "user_count" -> userCount)), INFO)
      }
    }

    println("[DEBUG] ===== STEP 4: Convert Course Enrollments to DataFrame =====")
    // Step 3: Convert course enrollments to DataFrame for joining
    val courseEnrollmentRows = courseEnrollmentsMap.flatMap { case (activityid, enrollments) =>
      enrollments.filter(_._4.nonEmpty).map { case (courseId, courseCode, courseName, userid, activityid, status) =>
        (userid, activityid, courseId, courseCode, courseName, status.toInt)
      }
    }.toSeq

    println(s"[DEBUG] Course enrollment rows count: ${courseEnrollmentRows.size}")

    val courseEnrollmentDF = if (courseEnrollmentRows.nonEmpty) {
      val df = courseEnrollmentRows.toDF("userid", "activityid", "courseid", "course_code", "course_name", "course_status")
      println(s"[DEBUG] Course enrollment DataFrame count: ${df.count()}")
      df.show(10, false)
      df
    } else {
      println("[DEBUG] No course enrollments found, creating empty DataFrame")
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
        new StructType()
          .add("userid", "string")
          .add("activityid", "string")
          .add("courseid", "string")
          .add("course_code", "string")
          .add("course_name", "string")
          .add("course_status", "int")
      )
    }

    println("[DEBUG] ===== STEP 5: Explode Activities and Join with Courses =====")
    // Step 4: Join course enrollment data back with activity info
    val activitiesExploded = decryptedSummary
      .select(
        col("userid"),
        col("firstname"),
        col("lastname"),
        col("username"),
        col("email"),
        col("usertype"),
        col("cin"),
        col("fmpsid"),
        col("province"),
        col("designation"),
        col("training_group"),
        col("orgname"),
        explode(col("activities_enrolled")).as("activity_info")
      )
      .select(
        col("userid"),
        col("firstname"),
        col("lastname"),
        col("username"),
        col("email"),
        col("usertype"),
        col("cin"),
        col("fmpsid"),
        col("province"),
        col("designation"),
        col("training_group"),
        col("orgname"),
        col("activity_info.activityid").as("activityid"),
        col("activity_info.batchid").as("batchid"),
        col("activity_info.batch_name").as("batch_name"),
        col("activity_info.activitytype").as("activitytype"),
        col("activity_info.start_date").as("start_date"),
        col("activity_info.end_date").as("end_date"),
        col("activity_info.enrolled_date").as("enrolled_date")
      )

    println(s"[DEBUG] Activities exploded count: ${activitiesExploded.count()}")
    activitiesExploded.show(5, false)

    // Debug: Check how many CL activities we have
    println("[DEBUG] Activity type distribution after explode:")
    activitiesExploded.groupBy("activitytype").count().show(false)
    println("[DEBUG] CL activities:")
    activitiesExploded.filter(lower(col("activitytype")).contains("competency level") ||
        lower(col("activitytype")).contains("competencylevel"))
      .select("userid", "activityid", "batchid", "activitytype", "batch_name")
      .show(10, false)

    // CRITICAL FIX: Use LEFT join to keep ALL activities (including CL without courses)
    val joinedWithCourses = activitiesExploded.join(
      courseEnrollmentDF,
      Seq("userid", "activityid"),
      "left"  // This ensures CL activities are kept even without course enrollments
    )

    println(s"[DEBUG] Joined with courses count: ${joinedWithCourses.count()}")
    joinedWithCourses.show(5, false)

    // Debug: Check CL activities after join
    println("[DEBUG] CL activities after join with courses:")
    joinedWithCourses.filter(lower(col("activitytype")).contains("competency level") ||
        lower(col("activitytype")).contains("competencylevel"))
      .select("userid", "activityid", "batchid", "activitytype", "batch_name", "courseid", "course_status")
      .show(10, false)

    // Create course_batch_info struct with course status
    val withCourseInfo = joinedWithCourses
      .withColumn("course_batch_info", struct(
        col("activityid"),
        col("batchid"),
        col("batch_name"),
        col("activitytype"),
        col("start_date"),
        col("end_date"),
        col("enrolled_date"),
        col("courseid"),
        col("course_code"),
        col("course_name"),
        coalesce(col("course_status"), lit(0)).as("course_status")
      ))

    println("[DEBUG] ===== STEP 6: Aggregate Course Metrics Per User =====")
    // Aggregate course data per user based on COURSE status from ReportCluster
    // IMPORTANT: Keep ALL activity records (even those without courses) for CF/CL tracking
    val aggregated = withCourseInfo.groupBy("userid", "firstname", "lastname", "username", "email", "usertype",
      "cin", "fmpsid", "province", "designation", "training_group", "orgname").agg(
      countDistinct(when(col("courseid").isNotNull, col("courseid"))).as("num_courses_enrolled"),
      countDistinct(when(col("course_status") === 1, col("courseid"))).as("num_courses_started"),
      countDistinct(when(col("course_status") === 2, col("courseid"))).as("num_courses_completed"),
      // Collect ALL records including those without courseid (for CF/CL tracking)
      collect_list(col("course_batch_info")).as("all_course_data")
    )

    println(s"[DEBUG] Aggregated user data count: ${aggregated.count()}")
    aggregated.select("userid", "num_courses_enrolled", "num_courses_started", "num_courses_completed").show(10, false)

    // Debug: Check all_course_data for CL activities
    println("[DEBUG] Sample all_course_data (checking for CL):")
    aggregated.select("userid", "all_course_data")
      .head(3)
      .foreach { row =>
        val userid = row.getAs[String]("userid")
        val courseData = row.getAs[Seq[Row]]("all_course_data")
        println(s"[DEBUG] User $userid has ${courseData.size} course records")
        courseData.take(10).foreach { cd =>
          val activityType = Option(cd.getAs[String]("activitytype")).getOrElse("NULL")
          val batchId = Option(cd.getAs[String]("batchid")).getOrElse("NULL")
          val activityId = Option(cd.getAs[String]("activityid")).getOrElse("NULL")
          val courseId = Option(cd.getAs[String]("courseid")).getOrElse("NULL")
          println(s"[DEBUG]   - Type: $activityType, ActivityID: $activityId, BatchID: $batchId, CourseID: $courseId")
        }
      }

    println("[DEBUG] ===== STEP 7: Fetch Course Details for Enrichment =====")
    // Step 5: Fetch course and batch details for enrichment
    val allActivityIdsForDetails = allActivityIds
    val (courseDetailsMap, batchDetailsMap) = getCourseDetails(allActivityIdsForDetails, courseBatchDF)

    println(s"[DEBUG] Course details map size: ${courseDetailsMap.size}")
    println(s"[DEBUG] Batch details map size: ${batchDetailsMap.size}")

    val broadcastedCourseMap = spark.sparkContext.broadcast(courseDetailsMap)
    val broadcastedBatchMap = spark.sparkContext.broadcast(batchDetailsMap)

    println("[DEBUG] ===== STEP 8: Enrich Courses with CF/CL Names and Course Details =====")

    val enrichCoursesUDF = udf((allCourseData: Seq[Row]) => {
      if (allCourseData == null || allCourseData.isEmpty) {
        CourseMetrics(Seq.empty, Seq.empty, Seq.empty)
      } else {
        println(s"[DEBUG] enrichCoursesUDF: Processing ${allCourseData.size} course records")

        // ENHANCED: Group by normalized batchid (handle CL format) and batch_name
        val groupedByBatch = allCourseData.filter(_ != null).groupBy { courseStruct =>
          val batchId = Option(courseStruct.getAs[String]("batchid")).getOrElse("")
          // For Competency Level, extract only the first part before ':'
          val normalizedBatchId = if (batchId.contains(":")) {
            batchId.split(":")(0)
          } else {
            batchId
          }
          val batchName = Option(courseStruct.getAs[String]("batch_name")).getOrElse("")
          println(s"[DEBUG] Original batchId: $batchId, Normalized: $normalizedBatchId, Batch name: $batchName")
          (normalizedBatchId, batchName)
        }

        val enrolledBatches = scala.collection.mutable.Map[String, BatchInfo]()
        val startedBatches = scala.collection.mutable.Map[String, BatchInfo]()
        val completedBatches = scala.collection.mutable.Map[String, BatchInfo]()

        groupedByBatch.foreach { case ((normalizedBatchId, batchName), activities) =>
          if (batchName.nonEmpty && normalizedBatchId.nonEmpty) {
            var cfId = ""
            var clId = ""
            var cfProgress = "0"
            var clProgress = "0"
            var enrolledDate = ""
            var startDate = ""
            var endDate = ""

            // Track CF and CL status separately
            var cfMaxStatus = 0
            var clMaxStatus = 0

            val enrolledCourses = scala.collection.mutable.ListBuffer[CourseDetail]()
            val startedCourses = scala.collection.mutable.ListBuffer[CourseDetail]()
            val completedCourses = scala.collection.mutable.ListBuffer[CourseDetail]()

            // ENHANCED: First pass - collect CF and CL IDs from ALL activities in this batch
            println(s"[DEBUG] ========== Processing Batch Group: $batchName (ID: $normalizedBatchId) ==========")
            println(s"[DEBUG] Number of activities in this batch group: ${activities.size}")

            activities.foreach { courseStruct =>
              val activityid = Option(courseStruct.getAs[String]("activityid")).getOrElse("")
              val batchid = Option(courseStruct.getAs[String]("batchid")).getOrElse("")
              val activityType = Option(courseStruct.getAs[String]("activitytype")).getOrElse("")
              val courseStatus = Option(courseStruct.getAs[Int]("course_status")).getOrElse(0)

              println(s"[DEBUG] Activity Details - ID: $activityid, Type: '$activityType', BatchID: $batchid, Status: $courseStatus")

              // Normalize activity type - handle variations
              val normalizedType = activityType.toLowerCase.trim.replaceAll("\\s+", "")

              println(s"[DEBUG] Normalized Type: '$normalizedType'")

              normalizedType match {
                case "competencyframework" =>
                  if (cfId.isEmpty) {
                    cfId = activityid
                    println(s"[DEBUG] *** SET CF_ID: $cfId ***")
                  }
                  if (courseStatus > cfMaxStatus) cfMaxStatus = courseStatus
                  println(s"[DEBUG] Found CF - ActivityID: $activityid, BatchID: $batchid, Status: $courseStatus, MaxStatus: $cfMaxStatus")
                case "competencylevel" =>
                  if (clId.isEmpty) {
                    clId = activityid
                    println(s"[DEBUG] *** SET CL_ID: $clId ***")
                  }
                  if (courseStatus > clMaxStatus) clMaxStatus = courseStatus
                  println(s"[DEBUG] Found CL - ActivityID: $activityid, BatchID: $batchid (original), Status: $courseStatus, MaxStatus: $clMaxStatus")
                case _ =>
                  println(s"[DEBUG] Unrecognized activity type: '$normalizedType' (original: '$activityType')")
              }
            }

            // Calculate progress based on max status for each type
            cfProgress = if (cfMaxStatus == 2) "100" else if (cfMaxStatus == 1) "50" else "0"
            clProgress = if (clMaxStatus == 2) "100" else if (clMaxStatus == 1) "50" else "0"

            println(s"[DEBUG] Batch: $batchName, CF_ID: $cfId (progress: $cfProgress), CL_ID: $clId (progress: $clProgress)")

            // Second pass - collect course details and other metadata
            activities.foreach { courseStruct =>
              val startDateVal = Option(courseStruct.getAs[String]("start_date")).getOrElse("")
              val endDateVal = Option(courseStruct.getAs[String]("end_date")).getOrElse("")
              val enrolledDateVal = Option(courseStruct.getAs[String]("enrolled_date")).getOrElse("")
              val courseStatus = Option(courseStruct.getAs[Int]("course_status")).getOrElse(0)
              val courseid = Option(courseStruct.getAs[String]("courseid")).getOrElse("")
              val courseCode = Option(courseStruct.getAs[String]("course_code")).getOrElse("")
              val courseName = Option(courseStruct.getAs[String]("course_name")).getOrElse("")

              // Set common values (take first non-empty)
              if (startDate.isEmpty && startDateVal.nonEmpty) startDate = startDateVal
              if (endDate.isEmpty && endDateVal.nonEmpty) endDate = endDateVal
              if (enrolledDate.isEmpty && enrolledDateVal.nonEmpty) enrolledDate = enrolledDateVal

              // Add course to appropriate lists based on status (only if courseid exists)
              if (courseid.nonEmpty) {
                val courseDetail = CourseDetail(courseid, courseCode, courseName)

                // ALL courses go to enrolled list (status 0, 1, or 2)
                if (!enrolledCourses.exists(_.courseid == courseid)) {
                  enrolledCourses += courseDetail
                }

                // Status 1: Also add to started list
                if (courseStatus == 1) {
                  if (!startedCourses.exists(_.courseid == courseid)) {
                    startedCourses += courseDetail
                  }
                }
                // Status 2: Also add to completed list
                else if (courseStatus == 2) {
                  if (!completedCourses.exists(_.courseid == courseid)) {
                    completedCourses += courseDetail
                  }
                }
              }
            }

            // CRITICAL: Create batch info even if no courses (for CF/CL tracking)
            // Always create enrolled batch entry with CF and CL IDs
            enrolledBatches(batchName) = BatchInfo(
              batchName, cfId, clId, startDate, endDate, enrolledDate,
              cfProgress, clProgress, enrolledCourses.toSeq
            )

            if (startedCourses.nonEmpty) {
              startedBatches(batchName) = BatchInfo(
                batchName, cfId, clId, startDate, endDate, enrolledDate,
                cfProgress, clProgress, startedCourses.toSeq
              )
            }

            if (completedCourses.nonEmpty) {
              completedBatches(batchName) = BatchInfo(
                batchName, cfId, clId, startDate, endDate, enrolledDate,
                cfProgress, clProgress, completedCourses.toSeq
              )
            }

            println(s"[DEBUG] Batch $batchName final state - CF: $cfId, CL: $clId, Enrolled Courses: ${enrolledCourses.size}, Started: ${startedCourses.size}, Completed: ${completedCourses.size}")
          }
        }

        println(s"[DEBUG] Total batches created - Enrolled: ${enrolledBatches.size}, Started: ${startedBatches.size}, Completed: ${completedBatches.size}")
        enrolledBatches.values.foreach { batch =>
          println(s"[DEBUG] Enrolled Batch: ${batch.batch_name}, CF: ${batch.cf_id}, CL: ${batch.cl_id}")
        }

        CourseMetrics(
          enrolledBatches.values.toSeq,
          startedBatches.values.toSeq,
          completedBatches.values.toSeq
        )
      }
    })

    // Apply the UDF to create course metrics
    val withEnriched = aggregated
      .withColumn("course_metrics_struct", enrichCoursesUDF(col("all_course_data")))

    val withCourseMetrics = withEnriched
      .withColumn("courses_enrolled", col("course_metrics_struct.courses_enrolled"))
      .withColumn("courses_started", col("course_metrics_struct.courses_started"))
      .withColumn("courses_completed", col("course_metrics_struct.courses_completed"))
      .withColumn(
        "course_metrics",
        to_json(struct(
          col("courses_enrolled"),
          col("courses_started"),
          col("courses_completed")
        ))
      )
      .drop("all_course_data", "course_metrics_struct", "courses_enrolled", "courses_started", "courses_completed")

    println("[DEBUG] ===== STEP 9: Final DataFrame =====")
    val finalDF = withCourseMetrics.select(reportCols.head, reportCols.tail: _*)
    println(s"[DEBUG] Final DataFrame count: ${finalDF.count()}")
    finalDF.select("userid", "num_courses_enrolled", "num_courses_started", "num_courses_completed").show(10, false)
    println(s"[DEBUG] Sample course_metrics:")
    finalDF.select("userid", "course_metrics").show(5, false)

    finalDF
  }

  def saveToPostgres(reportData: DataFrame): Unit = {
    val reportDataWithUpdate = reportData.withColumn("updated_date", current_timestamp())
    reportDataWithUpdate.write
      .mode("overwrite")
      .jdbc(url, requestsTable, connProperties)
  }


  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getCourseDetails(activityIds: List[String], courseBatchDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (Map[String, (String, String)], Map[(String, String), String]) = {
    if (activityIds.isEmpty) return (Map.empty, Map.empty)

    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val batchSize = 500
    val courseBatches = activityIds.distinct.grouped(batchSize).toList

    val courseDetails = courseBatches.flatMap { batch =>
      val searchFilter = Map(
        "request" -> Map(
          "filters" -> Map(
            "identifier" -> batch,
            "status" -> List("Live")
          ),
          "fields" -> List("name", "code", "identifier"),
          "limit" -> batch.size
        )
      )
      val request = JSONUtils.serialize(searchFilter)
      try {
        val response = RestUtil.post[Response](apiURL, request)
        if (response != null && response.result != null && response.result.content != null) {
          response.result.content
        } else {
          List.empty[CourseInfo]
        }
      } catch {
        case e: Exception =>
          JobLogger.log("Error fetching course details from API", Option(Map("error" -> e.getMessage)), INFO)
          List.empty[CourseInfo]
      }
    }

    // Course details map: activityid -> (code, name)
    val courseMap = courseDetails.map(c => c.identifier -> (c.code, c.name)).toMap

    // Batch details map: (activityId, batchId) -> batchName
    val batchMap: Map[(String, String), String] = courseBatchDF
      .select(col("activityid"), col("batchid"), col("name").as("batch_name"))
      .collect()
      .map(r => ((r.getAs[String]("activityid"), r.getAs[String]("batchid")), r.getAs[String]("batch_name")))
      .toMap

    (courseMap, batchMap)
  }

  // Date formatting methods
  def getDateFormat(): SimpleDateFormat = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
    dateFormatter
  }

  def formatDate(date: String): String = {
    Option(date).map(x => {
      getDateFormat().format(getDateFormat().parse(x))
    }).orNull
  }

  def convertDateFn: String => String = (date: String) => {
    Option(date).map(x => {
      val utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      utcDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
      getDateFormat().format(utcDateFormatter.parse(x))
    }).orNull
  }

  // Function to format completion date from ISO format to readable format
  def formatCompletionDate(date: String): String = {
    Option(date).map(x => {
      try {
        val isoFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        isoFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
        val parsedDate = isoFormatter.parse(x)

        val istFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        istFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
        istFormatter.format(parsedDate)
      } catch {
        case _: Exception => x // Return original if parsing fails
      }
    }).orNull
  }

  // Function to get course identifiers from hierarchy with user enrollment status
  def getCourseIdentifiers(activityid: String)(implicit spark: SparkSession, fc: FrameworkContext): List[(String, String, String, String, String, String)] = {
    try {
      println(s"[DEBUG] Starting getCourseIdentifiers for activityid: $activityid")
      JobLogger.log(s"Starting getCourseIdentifiers for activityid: $activityid",
        Option(Map("activityid" -> activityid)), INFO)

      // Fetch hierarchy data from database
      val hierarchyDF = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(hierarchyStoreSettings)
        .load()
        .filter(col("identifier") === activityid)
        .select("hierarchy")

      val hierarchyCount = hierarchyDF.count()
      println(s"[DEBUG] Hierarchy records found: $hierarchyCount")

      if (hierarchyCount == 0) {
        println(s"[DEBUG] No hierarchy found for activityid: $activityid")
        JobLogger.log(s"No hierarchy found for activityid: $activityid",
          Option(Map("activityid" -> activityid)), INFO)
        return List.empty
      }

      val hierarchyRow = hierarchyDF.first()
      val hierarchyJson = hierarchyRow.getAs[String]("hierarchy")

      println(s"[DEBUG] Hierarchy JSON length: ${hierarchyJson.length} characters")
      println(s"[DEBUG] Hierarchy JSON preview: ${hierarchyJson.take(500)}...")

      // Parse the hierarchy JSON
      val hierarchyMap = JSONUtils.deserialize[Map[String, Any]](hierarchyJson)
      println(s"[DEBUG] Parsed hierarchy map keys: ${hierarchyMap.keys.mkString(", ")}")

      // Search for courses in the hierarchy
      val courses = searchCoursesInHierarchy(hierarchyMap)

      if (courses.isEmpty) {
        println(s"[DEBUG] No courses found in hierarchy for activityid: $activityid")
        JobLogger.log(s"No courses found in hierarchy for activityid: $activityid",
          Option(Map("activityid" -> activityid)), INFO)
        return List.empty
      }

      // Extract course IDs
      val courseIds = courses.map(_._1)
      println(s"[DEBUG] Found ${courseIds.size} courses in hierarchy:")
      courses.foreach { case (id, code, name) =>
        println(s"[DEBUG]   - Course ID: $id, Code: $code, Name: $name")
      }

      JobLogger.log(s"Found ${courseIds.size} courses in hierarchy for activityid $activityid: ${courseIds.mkString(", ")}",
        Option(Map("activityid" -> activityid, "course_count" -> courseIds.size, "course_ids" -> courseIds.mkString(","))), INFO)

      // Query user enrollments using COURSEID (not activityid)
      println(s"[DEBUG] Querying ReportCluster user_enrolments for courseids: ${courseIds.mkString(", ")}")
      println(s"[DEBUG] ReportCluster settings: ${userCourseDBSettings}")

      val userEnrollmentDF = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(userCourseDBSettings)
        .load()

      println(s"[DEBUG] Total records in user_enrolments table: ${userEnrollmentDF.count()}")
      println(s"[DEBUG] Available columns: ${userEnrollmentDF.columns.mkString(", ")}")

      // Show sample data for debugging
      println(s"[DEBUG] Sample records from user_enrolments:")
      userEnrollmentDF.select("userid", "courseid", "status", "active").show(5, false)

      // Filter for our specific courses
      val filteredEnrollmentDF = userEnrollmentDF
        .filter(col("courseid").isin(courseIds: _*) && lower(col("active")).equalTo("true"))
        .select("userid", "courseid", "status")

      val filteredCount = filteredEnrollmentDF.count()
      println(s"[DEBUG] Filtered enrollment records count: $filteredCount")

      if (filteredCount > 0) {
        println(s"[DEBUG] Filtered enrollment records:")
        filteredEnrollmentDF.show(false)
      }

      val userEnrollments = filteredEnrollmentDF.collect().map { row =>
        val userid = row.getAs[String]("userid")
        val courseid = row.getAs[String]("courseid")
        val status = row.getAs[Int]("status")
        println(s"[DEBUG] Enrollment found - UserID: $userid, CourseID: $courseid, Status: $status")
        (userid, courseid, status)
      }.toList

      println(s"[DEBUG] Total user enrollments collected: ${userEnrollments.size}")
      JobLogger.log(s"Found ${userEnrollments.size} user enrollments for courses in activityid $activityid",
        Option(Map("activityid" -> activityid, "enrollment_count" -> userEnrollments.size)), INFO)

      // Combine course information with user enrollment data including status
      val coursesWithEnrollments = courses.flatMap { course =>
        val (courseId, courseCode, courseName) = course
        println(s"[DEBUG] Processing course: $courseId ($courseName)")

        // Find all users enrolled in this specific course
        val enrolledUsers = userEnrollments.filter(_._2 == courseId)
        println(s"[DEBUG]   - Enrolled users for $courseId: ${enrolledUsers.size}")

        if (enrolledUsers.nonEmpty) {
          enrolledUsers.map { case (userid, _, status) =>
            println(s"[DEBUG]   - User $userid enrolled in course $courseId with status $status")
            JobLogger.log(s"User $userid enrolled in course $courseId (status: $status) for activity $activityid",
              Option(Map("userid" -> userid, "courseid" -> courseId, "activityid" -> activityid, "status" -> status)), INFO)
            // Return: (courseId, courseCode, courseName, userid, activityid, status)
            (courseId, courseCode, courseName, userid, activityid, status.toString)
          }
        } else {
          // If no enrollments found for this course, return course info with empty user data
          println(s"[DEBUG]   - No enrollments found for course $courseId")
          JobLogger.log(s"No enrollments found for course $courseId in activity $activityid",
            Option(Map("courseid" -> courseId, "activityid" -> activityid)), INFO)
          List((courseId, courseCode, courseName, "", activityid, "0"))
        }
      }

      println(s"[DEBUG] Final coursesWithEnrollments count: ${coursesWithEnrollments.size}")
      println(s"[DEBUG] Completed getCourseIdentifiers for activityid: $activityid")
      println("=" * 80)

      coursesWithEnrollments
    } catch {
      case e: Exception =>
        println(s"[ERROR] Exception in getCourseIdentifiers for activityid: $activityid")
        println(s"[ERROR] Message: ${e.getMessage}")
        println(s"[ERROR] Stack trace:")
        e.printStackTrace()

        JobLogger.log(s"Error fetching hierarchy for activityid: $activityid",
          Option(Map("error" -> e.getMessage, "stacktrace" -> e.getStackTrace.mkString("\n"), "activityid" -> activityid)), INFO)
        List.empty
    }
  }

  // Helper function to recursively search for courses in hierarchy
  def searchCoursesInHierarchy(json: Map[String, Any]): List[(String, String, String)] = {
    var courses = List[(String, String, String)]()

    // Check current node for Course primary category
    val primaryCategory = json.get("primaryCategory").map(_.toString).getOrElse("")
    val identifier = json.get("identifier").map(_.toString).getOrElse("")
    val name = json.get("name").map(_.toString).getOrElse("")

    println(s"[DEBUG] Checking node - Name: $name, PrimaryCategory: $primaryCategory, Identifier: $identifier")

    if (primaryCategory == "Course") {
      val code = json.get("code").map(_.toString).getOrElse("")

      if (identifier.nonEmpty) {
        println(s"[DEBUG] *** COURSE FOUND *** Name: $name, ID: $identifier, Code: $code")
        courses = courses :+ (identifier, code, name)
        JobLogger.log(s"Found course in hierarchy: $name ($identifier)",
          Option(Map("identifier" -> identifier, "code" -> code, "name" -> name)), INFO)
      }
    }

    // Recurse into children - handle different collection types
    json.get("children") match {
      case Some(children) =>
        println(s"[DEBUG] Node '$name' has children, recursing...")
        children match {
          case childList: Seq[_] =>
            println(s"[DEBUG] Processing ${childList.size} children")
            childList.foreach {
              case childMap: Map[String, Any] @unchecked =>
                courses ++= searchCoursesInHierarchy(childMap)
              case _ =>
                println(s"[DEBUG] Skipping non-map child")
            }
          case _ =>
            println(s"[DEBUG] Children is not a Seq, skipping")
        }
      case None =>
        println(s"[DEBUG] Node '$name' has no children")
    }

    courses
  }
}