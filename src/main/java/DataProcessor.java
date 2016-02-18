import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.SystemClock;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by podolsky on 13.02.16.
 */
public class DataProcessor {

    private DataProvider dataProvider;
    private JavaSparkContext sparkContext;
    private ArrayList<Map<String,String>> errorCodeListOfMaps;

    DataProcessor(DataProvider dataProvider, JavaSparkContext sparkContext) {
        this.dataProvider = dataProvider;
        this.sparkContext = sparkContext;

    }

    String errorSummary(ErrorSummaryJSONRequest query){

        //String[] tableQueryParameters = {"computingsite", "jobstatus", "produsername", "specialhandling","produsername", "jeditaskid", "taskid"};

        //JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");
        sparkContext.setLocalProperty("spark.driver.allowMultipleContexts","true");
        final SQLContext sqlContext = new SQLContext(sparkContext);

        String [] dataFolders = dataProvider.getDataFolders().get("ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4");

        if (dataFolders == null || dataFolders.length == 0)
            return "Error with Data Source";

        DataFrame sourceDataFrame = sqlContext.read().parquet(dataFolders[0]);
        for (int i = 1; i < dataFolders.length; i++)
            sourceDataFrame = sourceDataFrame.unionAll(sqlContext.read().parquet(dataFolders[0]));


        final Accumulator<Map<String, ErrorSummaryerrsByCount>> errorsSummaryAccByCount = sparkContext.accumulator( new HashMap<String,
                ErrorSummaryerrsByCount>(), "errsByCount", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByCount>());
        final Accumulator<Map<String, ErrorSummaryerrsBySite>> errorsSummaryAccBySite = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsBySite>(), "errsBySite", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsBySite>());
        final Accumulator<Map<String, ErrorSummaryerrsByTask>> errorsSummaryAccByTask = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsByTask>(), "errsByTask", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByTask>());
        final Accumulator<Map<String, ErrorSummaryerrsByUser>> errorsSummaryAccByUser = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsByUser>(), "errsByUser", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByUser>());

        final Accumulator<Map<String, Integer>> errorsSummaryAccBySiteJobs = sparkContext.accumulator(new HashMap<String, Integer>(), "errorsSummaryAccBySiteJobs", new MapIntegerAccumulator());
        final Accumulator<Map<String, Integer>> errorSummaryerrsByTaskJobs = sparkContext.accumulator(new HashMap<String, Integer>(), "ErrorSummaryerrsByTaskJobs", new MapIntegerAccumulator());

        ErrorsSummaryProcessorMap map = new ErrorsSummaryProcessorMap();
        map.setErrorsSummaryAccByCount(errorsSummaryAccByCount);
        map.setErrorsSummaryAccBySite(errorsSummaryAccBySite);
        map.setErrorsSummaryAccByTask(errorsSummaryAccByTask);
        map.setErrorsSummaryAccByUser(errorsSummaryAccByUser);
        map.setErrorsSummaryAccBySiteJobs(errorsSummaryAccBySiteJobs);
        map.seterrorSummaryerrsByTaskJobs(errorSummaryerrsByTaskJobs);

        sourceDataFrame.toJavaRDD().foreach(map);

        Map<String, Integer> example = errorSummaryerrsByTaskJobs.value();

        for (String name: example.keySet()){

            String key =name.toString();
            String value = example.get(name).toString();
            System.out.println(key + " " + value);


        }

        return null;
    }





    /*


def errorSummaryDict(request,jobs, tasknamedict, testjobs):
    """ take a job list and produce error summaries from it """
    errsByCount = {}
    errsBySite = {}
    errsByUser = {}
    errsByTask = {}
    sumd = {}
    ## histogram of errors vs. time, for plotting
    errHist = {}
    flist = [ 'cloud', 'computingsite', 'produsername', 'taskid', 'jeditaskid', 'processingtype', 'prodsourcelabel', 'transformation', 'workinggroup', 'specialhandling', 'jobstatus' ]

    print len(jobs)
    for job in jobs:
        if not testjobs:
            if job['jobstatus'] not in [ 'failed', 'holding' ]: continue
        site = job['computingsite']
        if 'cloud' in request.session['requestParams']:
            if site in homeCloud and homeCloud[site] != request.session['requestParams']['cloud']: continue
        user = job['produsername']
        taskname = ''
        if job['jeditaskid'] > 0:
            taskid = job['jeditaskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'jeditaskid'
        else:
            taskid = job['taskid']
            if taskid in tasknamedict:
                taskname = tasknamedict[taskid]
            tasktype = 'taskid'

        if 'modificationtime' in job:
            tm = job['modificationtime']
            if tm is not None:
                tm = tm - timedelta(minutes=tm.minute % 30, seconds=tm.second, microseconds=tm.microsecond)
                if not tm in errHist: errHist[tm] = 0
                errHist[tm] += 1

        ## Overall summary
        for f in flist:
            if job[f]:
                if f == 'taskid' and job[f] < 1000000 and 'produsername' not in request.session['requestParams']:
                    pass
                else:
                    if not f in sumd: sumd[f] = {}
                    if not job[f] in sumd[f]: sumd[f][job[f]] = 0
                    sumd[f][job[f]] += 1
        if job['specialhandling']:
            if not 'specialhandling' in sumd: sumd['specialhandling'] = {}
            shl = job['specialhandling'].split()
            for v in shl:
                if not v in sumd['specialhandling']: sumd['specialhandling'][v] = 0
                sumd['specialhandling'][v] += 1

        for err in errorcodelist:
            if job[err['error']] != 0 and  job[err['error']] != '' and job[err['error']] != None:
                errval = job[err['error']]
                ## error code of zero is not an error
                if errval == 0 or errval == '0' or errval == None: continue
                errdiag = ''
                try:
                    errnum = int(errval)
                    if err['error'] in errorCodes and errnum in errorCodes[err['error']]:
                        errdiag = errorCodes[err['error']][errnum]
                except:
                    errnum = errval
                errcode = "%s:%s" % ( err['name'], errnum )
                if err['diag']:
                    errdiag = job[err['diag']]

                if errcode not in errsByCount:
                    errsByCount[errcode] = {}
                    errsByCount[errcode]['error'] = errcode
                    errsByCount[errcode]['codename'] = err['error']
                    errsByCount[errcode]['codeval'] = errnum
                    errsByCount[errcode]['diag'] = errdiag
                    errsByCount[errcode]['count'] = 0
                errsByCount[errcode]['count'] += 1

                if user not in errsByUser:
                    errsByUser[user] = {}
                    errsByUser[user]['name'] = user
                    errsByUser[user]['errors'] = {}
                    errsByUser[user]['toterrors'] = 0
                if errcode not in errsByUser[user]['errors']:
                    errsByUser[user]['errors'][errcode] = {}
                    errsByUser[user]['errors'][errcode]['error'] = errcode
                    errsByUser[user]['errors'][errcode]['codename'] = err['error']
                    errsByUser[user]['errors'][errcode]['codeval'] = errnum
                    errsByUser[user]['errors'][errcode]['diag'] = errdiag
                    errsByUser[user]['errors'][errcode]['count'] = 0
                errsByUser[user]['errors'][errcode]['count'] += 1
                errsByUser[user]['toterrors'] += 1

                if site not in errsBySite:
                    errsBySite[site] = {}
                    errsBySite[site]['name'] = site
                    errsBySite[site]['errors'] = {}
                    errsBySite[site]['toterrors'] = 0
                    errsBySite[site]['toterrjobs'] = 0
                if errcode not in errsBySite[site]['errors']:
                    errsBySite[site]['errors'][errcode] = {}
                    errsBySite[site]['errors'][errcode]['error'] = errcode
                    errsBySite[site]['errors'][errcode]['codename'] = err['error']
                    errsBySite[site]['errors'][errcode]['codeval'] = errnum
                    errsBySite[site]['errors'][errcode]['diag'] = errdiag
                    errsBySite[site]['errors'][errcode]['count'] = 0
                errsBySite[site]['errors'][errcode]['count'] += 1
                errsBySite[site]['toterrors'] += 1

                if tasktype == 'jeditaskid' or taskid > 1000000 or 'produsername' in request.session['requestParams']:
                    if taskid not in errsByTask:
                        errsByTask[taskid] = {}
                        errsByTask[taskid]['name'] = taskid
                        errsByTask[taskid]['longname'] = taskname
                        errsByTask[taskid]['errors'] = {}
                        errsByTask[taskid]['toterrors'] = 0
                        errsByTask[taskid]['toterrjobs'] = 0
                        errsByTask[taskid]['tasktype'] = tasktype
                    if errcode not in errsByTask[taskid]['errors']:
                        errsByTask[taskid]['errors'][errcode] = {}
                        errsByTask[taskid]['errors'][errcode]['error'] = errcode
                        errsByTask[taskid]['errors'][errcode]['codename'] = err['error']
                        errsByTask[taskid]['errors'][errcode]['codeval'] = errnum
                        errsByTask[taskid]['errors'][errcode]['diag'] = errdiag
                        errsByTask[taskid]['errors'][errcode]['count'] = 0
                    errsByTask[taskid]['errors'][errcode]['count'] += 1
                    errsByTask[taskid]['toterrors'] += 1
        if site in errsBySite: errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask: errsByTask[taskid]['toterrjobs'] += 1


    ## reorganize as sorted lists
    errsByCountL = []
    errsBySiteL = []
    errsByUserL = []
    errsByTaskL = []

    kys = errsByCount.keys()
    kys.sort()
    for err in kys:
        errsByCountL.append(errsByCount[err])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByCountL = sorted(errsByCountL, key=lambda x:-x['count'])

    kys = errsByUser.keys()
    kys.sort()
    for user in kys:
        errsByUser[user]['errorlist'] = []
        errkeys = errsByUser[user]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsByUser[user]['errorlist'].append(errsByUser[user]['errors'][err])
        errsByUserL.append(errsByUser[user])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByUserL = sorted(errsByUserL, key=lambda x:-x['toterrors'])

    kys = errsBySite.keys()
    kys.sort()
    for site in kys:
        errsBySite[site]['errorlist'] = []
        errkeys = errsBySite[site]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsBySite[site]['errorlist'].append(errsBySite[site]['errors'][err])
        errsBySiteL.append(errsBySite[site])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsBySiteL = sorted(errsBySiteL, key=lambda x:-x['toterrors'])

    kys = errsByTask.keys()
    kys.sort()
    for taskid in kys:
        errsByTask[taskid]['errorlist'] = []
        errkeys = errsByTask[taskid]['errors'].keys()
        errkeys.sort()
        for err in errkeys:
            errsByTask[taskid]['errorlist'].append(errsByTask[taskid]['errors'][err])
        errsByTaskL.append(errsByTask[taskid])
    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        errsByTaskL = sorted(errsByTaskL, key=lambda x:-x['toterrors'])

    suml = []
    for f in sumd:
        itemd = {}
        itemd['field'] = f
        iteml = []
        kys = sumd[f].keys()
        kys.sort()
        for ky in kys:
            iteml.append({ 'kname' : ky, 'kvalue' : sumd[f][ky] })
        itemd['list'] = iteml
        suml.append(itemd)
    suml = sorted(suml, key=lambda x:x['field'])

    if 'sortby' in request.session['requestParams'] and request.session['requestParams']['sortby'] == 'count':
        for item in suml:
            item['list'] = sorted(item['list'], key=lambda x:-x['kvalue'])

    kys = errHist.keys()
    kys.sort()
    errHistL = []
    for k in kys:
        errHistL.append( [ k, errHist[k] ] )

    return errsByCountL, errsBySiteL, errsByUserL, errsByTaskL, suml, errHistL













        tasknamedict = taskNameDict(jobs)
        def taskNameDict(jobs):
    ## Translate IDs to names. Awkward because models don't provide foreign keys to task records.
    taskids = {}
    jeditaskids = {}
    for job in jobs:
        if 'taskid' in job and job['taskid'] and job['taskid'] > 0:
            taskids[job['taskid']] = 1
        if 'jeditaskid' in job and job['jeditaskid'] and job['jeditaskid'] > 0: jeditaskids[job['jeditaskid']] = 1
    taskidl = taskids.keys()
    jeditaskidl = jeditaskids.keys()
    tasknamedict = {}
    if len(jeditaskidl) > 0:
        tq = { 'jeditaskid__in' : jeditaskidl }
        jeditasks = JediTasks.objects.filter(**tq).values('taskname', 'jeditaskid')
        for t in jeditasks:
            tasknamedict[t['jeditaskid']] = t['taskname']

    #if len(taskidl) > 0:
    #    tq = { 'taskid__in' : taskidl }
    #    oldtasks = Etask.objects.filter(**tq).values('taskname', 'taskid')
    #    for t in oldtasks:
    #        tasknamedict[t['taskid']] = t['taskname']
    return tasknamedict





     */


}
