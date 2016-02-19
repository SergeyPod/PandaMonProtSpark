import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Decimal;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by spadolski on 2/16/16.
 */
public class DistribPreProcessor implements Function<Row, Row> {

    private Map<String,String> homeClouds;
    DistribPreProcessor(Map<String,String> homeClouds){
        this.homeClouds = homeClouds;
    }

    private boolean isEventService(Row row) {
        String specialHandling = row.getAs("SPECIALHANDLING");
        if ((specialHandling != null) && (specialHandling.length() > 0))
            if (specialHandling.contains("eventservice") | specialHandling.contains("esmerge"))
                return true;
        return false;
    }

    private String getHomeCloud(String str) {
        if (homeClouds.containsKey("str)"))
            return homeClouds.get(str);
        else
            return null;
    }

    private String getErrorDescription(Row row, String mode) {
        //String mode = "html";
        String txt = "";
        //if ( ((String) row.getAs("BROKERAGEERRORDIAG")).length() >0)


                /*
                def getErrorDescription(job, mode='html'):
                txt = ''
                if 'metastruct' in job and job['metastruct']['exitCode'] != 0:
                    meta = job['metastruct']
                    txt += "%s: %s" % (meta['exitAcronym'], meta['exitMsg'])
                    return txt

                for errcode in errorCodes.keys():
                    errval = 0
                    if job.has_key(errcode):
                        errval = job[errcode]
                        if errval != 0 and errval != '0' and errval != None and errval != '':
                            try:
                                errval = int(errval)
                            except:
                                pass # errval = -1
                            errdiag = errcode.replace('errorcode','errordiag')
                            if errcode.find('errorcode') > 0:
                                diagtxt = job[errdiag]
                            else:
                                diagtxt = ''
                            if len(diagtxt) > 0:
                                desc = diagtxt
                            elif errval in errorCodes[errcode]:
                                desc = errorCodes[errcode][errval]
                            else:
                                desc = "Unknown %s error code %s" % ( errcode, errval )
                            errname = errcode.replace('errorcode','')
                            errname = errname.replace('exitcode','')
                            if mode == 'html':
                                txt += " <b>%s:</b> %s" % ( errname, desc )
                            else:
                                txt = "%s: %s" % ( errname, desc )
                return txt
                 */


        return txt;
    }


    private String errorInfo(Row row, int nchars, String mode) {
        String errtxt = "";
        String err1 = "";


        if (  ((BigDecimal) row.getAs("BROKERAGEERRORCODE")).longValue() != 0) {
            errtxt += "Brokerage error " + row.getAs("BROKERAGEERRORCODE") + ": " + row.getAs("BROKERAGEERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = "Broker: " + row.getAs("BROKERAGEERRORDIAG") + "";
        }
        if ( ((BigDecimal) row.getAs("DDMERRORCODE")).longValue() != 0) {
            errtxt += "'DDM error " + row.getAs("DDMERRORCODE") + ": " + ( (  ErrorsSummaryProcessorMap.checkIsField(row, "DDMERRORDIAG") )? row.getAs("DDMERRORDIAG") : "") + " <br>";
            if (err1.equals("")) err1 = "DDM: " + ( (  ErrorsSummaryProcessorMap.checkIsField(row, "DDMERRORDIAG") )? row.getAs("DDMERRORDIAG") : "");
        }
        if ( ((BigDecimal) row.getAs("EXEERRORCODE")).longValue() != 0) {
            errtxt += "'Executable error " + row.getAs("EXEERRORCODE") + ": " + row.getAs("EXEERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = "Exe: " + row.getAs("EXEERRORDIAG");
        }


        if ( ((BigDecimal) row.getAs("JOBDISPATCHERERRORCODE")).longValue() != 0) {
            errtxt += "'Dispatcher error " + row.getAs("JOBDISPATCHERERRORCODE") + ": " + row.getAs("JOBDISPATCHERERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = "Dispatcher: " + row.getAs("JOBDISPATCHERERRORDIAG");
        }

        if ( ((BigDecimal) row.getAs("PILOTERRORCODE")).longValue() != 0) {
            errtxt += "'Pilot error " + row.getAs("PILOTERRORCODE") + ": " + row.getAs("PILOTERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = "Pilot: " + row.getAs("PILOTERRORDIAG");
        }
        if ( ((BigDecimal) row.getAs("SUPERRORCODE")).longValue() != 0) {
            errtxt += "'Pilot error " + row.getAs("SUPERRORCODE") + ": " + row.getAs("SUPERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = row.getAs("SUPERRORDIAG");
        }
        if ( ((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() != 0) {
            errtxt += "'Task buffer error " + row.getAs("TASKBUFFERERRORCODE") + ": " + row.getAs("TASKBUFFERERRORDIAG") + " <br>";
            if (err1.equals("")) err1 = row.getAs("TASKBUFFERERRORDIAG");
        }
        if ( row.getAs("TRANSEXITCODE") != null ) {
            errtxt += "'Trf exit code " + row.getAs("TRANSEXITCODE") + ": " + row.getAs("TRANSEXITCODE") + " <br>";
            if (err1.equals("")) err1 = "Trf exit code " + row.getAs("TRANSEXITCODE");
        }

        String desc = getErrorDescription(row, "html");
        if (desc.length() > 0) {
            errtxt += desc + "<br>";
            if (err1.equals("")) err1 = getErrorDescription(row, "string");
        }

        String ret;
        if (errtxt.length() > nchars)
            ret = errtxt.substring(0, nchars) + "...";
        else
            ret = errtxt.substring(0, errtxt.length());
        if (err1.contains("lost heartbeat")) err1 = "lost heartbeat";

        if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(err1, "unknown transexitcode"))
            err1 = "unknown transexit";

        if (err1.contains(" at ")) err1 = err1.substring(0, err1.indexOf(" at ") - 1);
        if (err1.contains("lost heartbeat")) err1 = "lost heartbeat";
        err1 = err1.replaceAll("\n", " ");

        if (mode.equals("html"))
            return errtxt;
        else
            return err1.substring(0, (errtxt.length() > nchars) ? nchars : errtxt.length());
    }


    public Row call(Row row) throws Exception {


        RowDataModifier rowDataModifier = new RowDataModifier(row);
        if (isEventService(row)) {
            if (((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() == 111) {
                rowDataModifier.addVal("TASKBUFFERERRORDIAG", "Rerun scheduled to pick up unprocessed events");
                rowDataModifier.addVal("PILOTERRORCODE", Decimal.apply(0));
                rowDataModifier.addVal("PILOTERRORDIAG", "Job terminated by signal from PanDA server");
                rowDataModifier.addVal("JOBSTATUS", "finished");
            }
            if (((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() == 112) {
                rowDataModifier.addVal("TASKBUFFERERRORDIAG", "All events processed, merge job created");
                rowDataModifier.addVal("PILOTERRORCODE", Decimal.apply(0));
                rowDataModifier.addVal("PILOTERRORDIAG", "Job terminated by signal from PanDA server");
                rowDataModifier.addVal("JOBSTATUS", "finished");
            }
            if (((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() == 114) {
                rowDataModifier.addVal("TASKBUFFERERRORDIAG", "No rerun to pick up unprocessed, at max attempts");
                rowDataModifier.addVal("PILOTERRORCODE", Decimal.apply(0));
                rowDataModifier.addVal("PILOTERRORDIAG", "Job terminated by signal from PanDA server");
                rowDataModifier.addVal("JOBSTATUS", "finished");
            }
            if (((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() == 115) {
                rowDataModifier.addVal("TASKBUFFERERRORDIAG", "No events remaining, other jobs still processing");
                rowDataModifier.addVal("PILOTERRORCODE", Decimal.apply(0));
                rowDataModifier.addVal("PILOTERRORDIAG", "Job terminated by signal from PanDA server");
                rowDataModifier.addVal("JOBSTATUS", "finished");
            }
            if (((BigDecimal) row.getAs("TASKBUFFERERRORCODE")).longValue() == 116) {
                rowDataModifier.addVal("TASKBUFFERERRORDIAG", "No remaining event ranges to allocate");
                rowDataModifier.addVal("PILOTERRORCODE", Decimal.apply(0));
                rowDataModifier.addVal("PILOTERRORDIAG", "Job terminated by signal from PanDA server");
                rowDataModifier.addVal("JOBSTATUS", "finished");
            }

            if (row.getAs("JOBMETRICS") != null) {
                Pattern pattern = Pattern.compile(".*mode\\=([^\\s]+).*HPCStatus\\=([A-Za-z0-9]+)");
                Matcher matcher = pattern.matcher((String) row.getAs("JOBMETRICS"));
                if (matcher.matches()) {
                    rowDataModifier.addVal("JOBMODE", matcher.group(1));
                    rowDataModifier.addVal("SUBSTATE", matcher.group(2));
                }
                pattern = Pattern.compile(".*coreCount\\=([0-9]+)");
                matcher = pattern.matcher((String) row.getAs("JOBMETRICS"));
                if (matcher.matches()) {
                    rowDataModifier.addVal("CORECOUNT",  Decimal.apply(new Integer(matcher.group(1)).intValue() ));
                }
            }
        }

        String destinationDBlock = row.getAs("DESTINATIONDBLOCK");
        if (destinationDBlock.length() > 0) {
            String destinationDBlockArr[] = destinationDBlock.split(".");
            if (destinationDBlockArr.length == 6)
                rowDataModifier.addVal("OUTPUTFILETYPE", destinationDBlockArr[4]);
            else if (destinationDBlockArr.length >= 7)
                rowDataModifier.addVal("OUTPUTFILETYPE", destinationDBlockArr[6]);
            else
                rowDataModifier.addVal("OUTPUTFILETYPE", "?");
        }

//        rowDataModifier.addVal("HOMECLOUD", getHomeCloud((String) row.getAs("COMPUTINGSITE")));

        rowDataModifier.addVal("HOMECLOUD", " ");

        if (((String) row.getAs("PRODUSERID")).length() > 0)
            rowDataModifier.addVal("PRODUSERNAME", row.getAs("PRODUSERID"));
        else
            rowDataModifier.addVal("PRODUSERNAME", row.getAs("Unknown"));

        String transformation = row.getAs("TRANSFORMATION");
        if (transformation.length() > 0) {
            String transformationArr[] = transformation.split("/");
            rowDataModifier.addVal("TRANSFORMATION", transformationArr[transformationArr.length - 1]);
        }

        String jobStatus = row.getAs("JOBSTATUS");
        if (((jobStatus.contains("failed")) | (jobStatus.contains("cancelled"))) & (((String) row.getAs("TRANSFORMATION")).length() > 0))
            rowDataModifier.addVal("ERRORINFO", errorInfo(row, 70, "html"));
        else
            rowDataModifier.addVal("ERRORINFO", " ");

        rowDataModifier.addVal("JOBINFO", " ");

        if (isEventService(row)) {
            String taskbuffererrordiag = row.getAs("TASKBUFFERERRORDIAG");
            if ((taskbuffererrordiag != null) && (taskbuffererrordiag.length() > 0))
                rowDataModifier.addVal("JOBINFO", "taskbuffererrordiag");
            else if (((String) row.getAs("SPECIALHANDLING")).length() > 0)
                rowDataModifier.addVal("JOBINFO", "Event service merge job");
            else
                rowDataModifier.addVal("JOBINFO", "Event service job");


                        /*
                                job['duration'] = ""
                                job['durationsec'] = 0
                                #if job['jobstatus'] in ['finished','failed','holding']:
                                if 'endtime' in job and 'starttime' in job and job['starttime']:
                                    starttime = job['starttime']
                                    if job['endtime']:
                                        endtime = job['endtime']
                                    else:
                                        endtime = timezone.now()

                                    duration = max(endtime - starttime, timedelta(seconds=0))
                                    ndays = duration.days
                                    strduration = str(timedelta(seconds=duration.seconds))
                                    job['duration'] = "%s:%s" % ( ndays, strduration )
                                    job['durationsec'] = ndays*24*3600+duration.seconds
                                             */

        }

        return rowDataModifier.compileRow();//RowFactory.create(row.get(0),row.get(1),row.get(2));
    }


}
