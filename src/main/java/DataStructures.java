import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by podolsky on 13.02.16.
 */

class ErrorSummaryJSONRequest {
    public String JOBType;
    public int hours;
    public String JOBTableKeysFilter[];
    public String JOBTableValuesFilter[];

}


class ErrorSummaryerrsByCount implements Serializable{
    public String error;
    public String codename;
    public String codeval;
    public String diag;
    public Integer count;
}

class ErrorSummaryerrsByUser extends ErrorSummaryerrs implements Serializable{
    public String name;
    public Integer toterrors;
}

class ErrorSummaryerrsBySite extends ErrorSummaryerrs implements Serializable {
    public Integer toterrjobs;
    public Integer toterrors;
    public String name;
}

class ErrorSummaryerrsByTask extends ErrorSummaryerrs implements Serializable{
    public BigDecimal name;
    public String longname;
    public Integer toterrors;
    public Integer toterrjobs;
    public String tasktype;
    public String errcode;
}

class ErrorSummaryerrs {
    public Map<String,ErrorSummaryerrsByCount> errors;
    public void mergeErrors( Map<String,ErrorSummaryerrsByCount> otherErrors) {

        if (errors != null)
        try {
            otherErrors.forEach((k, v) -> errors.merge(k, v, (obj1, obj2) ->
            {
                if (obj1 == null) return null;
                obj1.count += obj2.count;
                return obj1;
            }));
        } catch (NullPointerException ex){
            System.out.println(ex);
        }
    }
}








class JSONClasses {
}

class CONSTS {

        /*
    */

    public static String errorcodelist = "[{ \"name\" : \"brokerage\", \"error\" : \"brokerageerrorcode\", \"diag\" : \"brokerageerrordiag\" }, { \"name\" : \"ddm\", \"error\" : \"ddmerrorcode\", \"diag\" : \"ddmerrordiag\" }, { \"name\" : \"exe\", \"error\" : \"exeerrorcode\", \"diag\" : \"exeerrordiag\" }, { \"name\" : \"jobdispatcher\", \"error\" : \"jobdispatchererrorcode\", \"diag\" : \"jobdispatchererrordiag\" }, { \"name\" : \"pilot\", \"error\" : \"piloterrorcode\", \"diag\" : \"piloterrordiag\" }, { \"name\" : \"sup\", \"error\" : \"superrorcode\", \"diag\" : \"superrordiag\" }, { \"name\" : \"taskbuffer\", \"error\" : \"taskbuffererrorcode\", \"diag\" : \"taskbuffererrordiag\" }, { \"name\" : \"transformation\", \"error\" : \"transexitcode\", \"diag\" : None },]";
    public static String jSONClouds = "{\"WQCG-Harvard-OSG_SBGRID\": \"OSG\", \"ANALY_SWT2_CPB\": \"US\", \"ANALY_OX_SL6_GLEXEC\": \"UK\", \"CONNECT_PILE\": \"US\", \"IEPSAS-Kosice_MCORE\": \"DE\", \"ANALY_IEPSAS-Kosice_GLEXEC\": \"DE\", \"pic_MCORE\": \"ES\", \"IEPSAS-Kosice\": \"DE\", \"UKI-NORTHGRID-MAN-HEP_VAC\": \"UK\", \"INFN-NAPOLI-ATLAS_MCORE\": \"IT\", \"ANALY_ZA-WITS-CORE\": \"IT\", \"ANALY_INFN-NAPOLI_GLEXEC\": \"IT\", \"BNL_EC2E1_MCORE\": \"US\", \"TRIUMF_HIMEM\": \"CA\", \"BNL_EC2W1\": \"US\", \"UKI-SOUTHGRID-RALPP_SL6\": \"UK\", \"ANALY_UNLP\": \"ES\", \"ANALY_LAPP\": \"FR\", \"RDIGTEST\": \"US\", \"ANALY_IHEP_GLEXEC\": \"RU\", \"UKI-LT2-IC-HEP_SL6\": \"UK\", \"ANALY_MPPMU\": \"DE\", \"ANALY_MANC_SL6_GLEXEC\": \"UK\", \"ANALY_IAAS\": \"CA\", \"TWTEST\": \"US\", \"IN2P3-LPSC_CLOUD\": \"FR\", \"ANALY_INFN-BOLOGNA-T3\": \"IT\", \"BU_ATLAS_Tier2_LMEM\": \"US\", \"ANALY_BNL_GLEXEC\": \"US\", \"BNL_ATLAS_Install\": \"US\", \"LRZ-LMU_C2PAP_MCORE\": \"DE\", \"wuppertalprod_HI\": \"DE\", \"ANALY_FZK_SHORT\": \"DE\", \"ANALY_GRIF-IRFU_GLEXEC\": \"FR\", \"RAL-LCG2_CEPH\": \"UK\", \"TW-FTT_SL6\": \"TW\", \"ANALY_DESY-HH\": \"DE\", \"CA-MCGILL-CLUMEQ-T2_MCORE\": \"CA\", \"UTA_PAUL_TEST\": \"US\", \"DBCE_MCORE\": \"CERN\", \"ATLAS_OPP_OSG_ES\": \"US\", \"ANALY_LPSC\": \"FR\", \"ANALY_IL-TAU-HEP-CREAM\": \"NL\", \"AGLT2_TEST\": \"US\", \"CERN-P1_preprod_MCORE\": \"CERN\", \"UKI-LT2-QMUL_HIMEM_SL6\": \"UK\", \"ANALY_MCGILL\": \"CA\", \"ANALY_TOKYO\": \"FR\", \"CA-MCGILL-CLUMEQ-T2\": \"CA\", \"Prairiefire_SBGRID\": \"OSG\", \"ANALY_BNL_test2\": \"US\", \"ANALY_BNL_test3\": \"US\", \"RRC-KI_MCORE\": \"NL\", \"JINR_MCORE\": \"NL\", \"ORNL_Titan_install\": \"US\", \"ANALY_INFN-FRASCATI\": \"IT\", \"BNL_Test_2_CE_1\": \"US\", \"UKI-NORTHGRID-SHEF-HEP_SL6\": \"UK\", \"ANALY_SARA_hpc_cloud\": \"NL\", \"ANALY_TRIUMF_SL7\": \"CA\", \"ANALY_SLAC\": \"US\", \"ANALY_OX_SL6\": \"UK\", \"ANALY_TAIWAN_SL6_GLEXEC\": \"TW\", \"UFlorida-HPC_SBGRID\": \"OSG\", \"IN2P3-LAPP-TEST\": \"FR\", \"INFN-COSENZA-RECAS_MCORE\": \"IT\", \"ANALY_SLAC_LMEM\": \"US\", \"UKI-SCOTGRID-DURHAM_SL6\": \"UK\", \"ANALY_TRIUMF_PPS\": \"CA\", \"ANALY_IFAE\": \"ES\", \"INFN-BOLOGNA-T3\": \"IT\", \"CA-JADE\": \"CA\", \"ANALY_DRESDEN\": \"DE\", \"HPC2N_MCORE\": \"ND\", \"ANALY_ECDF_SL6\": \"UK\", \"ROMANIA07_MCORE\": \"FR\", \"IFAE\": \"ES\", \"NERSC_Edison\": \"US\", \"IL-TAU-HEP_MCORE\": \"NL\", \"ANALY_LAPP_TEST\": \"FR\", \"DESY-HH_TEST_MCORE\": \"DE\", \"ANALY_INFN-GENOVA\": \"IT\", \"RRC-KI-HPC2\": \"RU\", \"UKI-LT2-Brunel_SL6\": \"UK\", \"UKI-NORTHGRID-LANCS-HEP_VAC\": \"UK\", \"BNL_ITB_Install\": \"US\", \"ANALY_MANC_SL6\": \"UK\", \"ANALY_LUNARC\": \"ND\", \"UTA_SWT2_Install\": \"US\", \"GRIF-LAL_HTCondor_MCORE\": \"FR\", \"ANALY_DESY-HH_GLEXEC\": \"DE\", \"ANALY_INFN-BOLOGNA-T3_GLEXEC\": \"IT\", \"CSCS-LCG2-HPC_MCORE\": \"DE\", \"WT2_Install\": \"US\", \"BNL_PROD\": \"US\", \"ANALY_LPC_GLEXEC\": \"FR\", \"ANALY_AGLT2_TIER3_TEST\": \"US\", \"ANALY_AM-04-YERPHI\": \"NL\", \"UKI-SCOTGRID-ECDF_MCORE\": \"UK\", \"TECHNION-HEP\": \"NL\", \"ANALY_DCSC\": \"ND\", \"INFN-ROMA1\": \"IT\", \"INFN-ROMA3\": \"IT\", \"INFN-ROMA2\": \"IT\", \"ANALY_TEST-APF\": \"US\", \"ANALY_FZU\": \"DE\", \"Taiwan-LCG2_HIMEM\": \"TW\", \"BNL_EC2W2\": \"US\", \"ANALY_WEIZMANN-CREAM_GLEXEC\": \"NL\", \"ANALY_NCG-INGRID-PT_SL6_GLEXEC\": \"ES\", \"ANALY_FZK\": \"DE\", \"ANALY_INFN-MILANO-ATLASC\": \"IT\", \"ANALY_GRIF-LAL_GLEXEC\": \"FR\", \"CONNECT_ES_MCORE\": \"US\", \"EELA-UNLP\": \"ES\", \"ANALY_QMUL_TEST\": \"UK\", \"ANALY_AUSTRALIA_TEST\": \"CA\", \"ANALY_ORNL_Titan\": \"US\", \"TRIUMF_PPS\": \"CA\", \"ANALY_SARA_bignode\": \"NL\", \"CYFRONET-LCG2\": \"DE\", \"ANALY_BNL_EC2W2\": \"US\", \"ANALY_BNL_EC2W1\": \"US\", \"ANALY_TRIUMF_GLEXEC\": \"CA\", \"RRC-KI-T1_MCORE\": \"RU\", \"ANALY_TOKYO_HIMEM\": \"FR\", \"LRZ-LMU_MUC_MCORE1\": \"DE\", \"ANALY_FZU_GLEXEC\": \"DE\", \"BU_ATLAS_Tier2_SL6\": \"US\", \"ANALY_SiGNET\": \"ND\", \"ANALY_WISC_ATLAS\": \"US\", \"DESY-ZN_MCORE\": \"DE\", \"DUKE_ATLASGCE\": \"US\", \"HPC2N\": \"ND\", \"EELA-UTFSM_MCORE\": \"ES\", \"RRC-KI-T1_TEST\": \"RU\", \"UKI-SOUTHGRID-RALPP_MCORE\": \"UK\", \"SARA-MATRIX_LONG\": \"NL\", \"ANALY_VICTORIA_TEST\": \"CA\", \"ANALY_TAIWAN_SL6\": \"TW\", \"BNL_ATLAS_2\": \"US\", \"RAL-LCG2_HIMEM_SL6\": \"UK\", \"ANALY_MANC_SL6_SHORT\": \"UK\", \"ANALY_RRC-KI\": \"NL\", \"ANALY_SFU_TEST\": \"CA\", \"GRIF-IRFU\": \"FR\", \"CERN-PROD_PRESERVATION\": \"CERN\", \"ANALY_BNL_EC2E1\": \"US\", \"BNL_CLOUD\": \"US\", \"CHARMM\": \"OSG\", \"ANALY_SCINET_TEST\": \"CA\", \"CSCS-LCG2-HPC\": \"DE\", \"NERSC_Hopper\": \"US\", \"TECHNION-HEP_MCORE\": \"NL\", \"PNPI_PROD\": \"NL\", \"ANALY_INFN-LECCE\": \"IT\", \"UKI-LT2-QMUL_MCORE\": \"UK\", \"ANALY_GRIF-LPNHE_GLEXEC\": \"FR\", \"MWT2_CEPH\": \"US\", \"CERN-P1_preprod\": \"CERN\", \"FZK-LCG2\": \"DE\", \"IN2P3-CC-T3_VM02\": \"FR\", \"IN2P3-CC-T3_VM01\": \"FR\", \"ANALY_AUSTRALIA_GLEXEC\": \"CA\", \"HELIX_NEBULA_EGI\": \"CERN\", \"TRIUMF_SL7\": \"CA\", \"GOOGLE_COMPUTE_ENGINE\": \"US\", \"ANALY_TAIWAN_PNFS_SL6\": \"TW\", \"ANALY_RAL_MCORE\": \"UK\", \"UKI-LT2-Brunel_MCORE\": \"UK\", \"UKI-NORTHGRID-MAN-HEP_SL6\": \"UK\", \"wuppertalprod\": \"DE\", \"ANALY_RHUL_SL6_GLEXEC\": \"UK\", \"UKI-SOUTHGRID-OX-HEP_MCORE\": \"UK\", \"DCSC_MCORE\": \"ND\", \"ROMANIA07\": \"FR\", \"ROMANIA02\": \"FR\", \"OU_PAUL_TEST\": \"US\", \"GRIDPP_MCORE\": \"UK\", \"UKI-LT2-RHUL_MCORE\": \"UK\", \"ANALY_CAM_SL6_GLEXEC\": \"UK\", \"ANALY_DUKE\": \"US\", \"ANALY_GR-01-AUTH\": \"IT\", \"BNL_CLOUD_MCORE\": \"US\", \"ANALY_TECHNION-HEP-CREAM\": \"NL\", \"UKI-SCOTGRID-GLASGOW_SL6\": \"UK\", \"CA-VICTORIA-WESTGRID-T2\": \"CA\", \"TOKYO\": \"FR\", \"AGLT2_LMEM\": \"US\", \"SWT2_CPB_MCORE\": \"US\", \"CERN-P1_MCORE_HI\": \"CERN\", \"BNL_PROD_MCORE\": \"US\", \"MWT2_SL6\": \"US\", \"ANALY_CYF_GLEXEC\": \"DE\", \"ANALY_SLAC_SHORT_1HR\": \"US\", \"TestPilot\": \"US\", \"LPC\": \"FR\", \"BOINC-TEST\": \"CERN\", \"NCG-INGRID-PT_SL6\": \"ES\", \"ANALY_UAM_GLEXEC\": \"ES\", \"Australia-ATLAS\": \"CA\", \"ANALY_RALPP_SL6\": \"UK\", \"BU_ATLAS_Tier2_MCORE\": \"US\", \"UKI-LT2-QMUL_SL6\": \"UK\", \"CERN-PROD\": \"CERN\", \"ANALY_PIC_SL6\": \"ES\", \"ANALY_TAIWAN_TEST\": \"TW\", \"UIO_MCORE\": \"ND\", \"TESTGLEXEC\": \"US\", \"ANALY_IN2P3-CC_GLEXEC\": \"FR\", \"ARNES\": \"ND\", \"ROMANIA02_MCORE\": \"FR\", \"ARC-ES\": \"ND\", \"ANALY_UTA_PAUL_TEST\": \"US\", \"HELIX_NEBULA_ATOS\": \"CERN\", \"NIKHEF-ELPROD_LONG\": \"NL\", \"INFN-COSENZA\": \"IT\", \"UKI-LT2-UCL-HEP_VAC\": \"UK\", \"ANALY_ROMANIA07\": \"FR\", \"ANALY_IFIC\": \"ES\", \"ANALY_ROMANIA02\": \"FR\", \"INFN-GENOVA\": \"IT\", \"ITEP_PROD\": \"NL\", \"BNL_PROD_MCOREHIMEM\": \"US\", \"FZK-LCG2_MCORE\": \"DE\", \"LUNARC\": \"ND\", \"LAPP_MCORE\": \"FR\", \"ANALY_BNL_SHORT\": \"US\", \"GR-12-TEIKAV\": \"IT\", \"UKI-SOUTHGRID-BHAM-HEP_SL6\": \"UK\", \"INFN-NAPOLI-SCOPE\": \"IT\", \"ROMANIA14_MCORE\": \"FR\", \"ANALY_DESY-HH_TEST\": \"DE\", \"INFN-FRASCATI\": \"IT\", \"CERN-PROD_CLOUD\": \"CERN\", \"CONNECT_PILE_MCORE\": \"US\", \"INFN-T1_MCORE\": \"IT\", \"SiGNET\": \"ND\", \"ANALY_CERN_GLEXECDEV\": \"CERN\", \"ANALY_NIKHEF-ELPROD_GLEXEC\": \"NL\", \"ANALY_BNL_LONG\": \"US\", \"ANALY_QMUL_HIMEM_SL6\": \"UK\", \"DCSC\": \"ND\", \"INFN-MILANO-ATLASC_MCORE\": \"IT\", \"ANALY_Anselm_HPC\": \"DE\", \"RAL-LCG2_ES\": \"UK\", \"UNIBE-LHEP_CLOUD_MCORE\": \"ND\", \"INFN-ROMA3_MCORE\": \"IT\", \"ANALY_AUSTRALIA\": \"CA\", \"INFN-T1\": \"IT\", \"ANALY_INFN-BOLOGNA-T3_TEST\": \"IT\", \"BNL_OSG_1\": \"OSG\", \"ANALY_TECHNION-HEP-CREAM_GLEXEC\": \"NL\", \"ANALY_MWT2_CEPH\": \"US\", \"CA-VICTORIA-WESTGRID-T2_MCORE\": \"CA\", \"OU_OCHEP_SWT2_Install\": \"US\", \"ANALY_NSC\": \"ND\", \"ANALY_Arizona\": \"US\", \"SWT2_CPB\": \"US\", \"IFIC\": \"ES\", \"HU_ATLAS_Tier2_MCORE\": \"US\", \"ANALY_SARA_GLEXEC\": \"NL\", \"NSC_MCORE\": \"ND\", \"ANALY_CSCS\": \"DE\", \"ANALY_ARNES\": \"ND\", \"ANALY_CSCS_GLEXEC\": \"DE\", \"GoeGrid_MCORE\": \"DE\", \"ANALY_GRIF-IRFU\": \"FR\", \"ANALY_FMPhI-UNIBA\": \"DE\", \"DBCE\": \"CERN\", \"ANALY_CERN_SHORT\": \"CERN\", \"ANALY_UNIBE-LHEP\": \"ND\", \"ANALY_LONG_BNL_LOCAL\": \"US\", \"ANALY_LBNL_TEST_CLOUD\": \"US\", \"ANALY_IL-TAU-HEP-CREAM_GLEXEC\": \"NL\", \"UKI-SOUTHGRID-OX-HEP_VAC\": \"UK\", \"UTD-HEP_Install\": \"US\", \"SARA-MATRIX\": \"NL\", \"IN2P3-LPSC\": \"FR\", \"ANALY_JINR\": \"NL\", \"LBNL_DSD_ITB\": \"US\", \"IN2P3-LPSC_CLOUD_MCORE\": \"FR\", \"UKI-SCOTGRID-ECDF_TEST\": \"UK\", \"ANALY_TRIUMF_TEST\": \"CA\", \"TR-10-ULAKBIM_MCORE\": \"NL\", \"ANALY_LRZ\": \"DE\", \"IFAE_MCORE\": \"ES\", \"UKI-SOUTHGRID-OX-HEP_CLOUD\": \"UK\", \"ANALY_DUKE_CLOUD\": \"US\", \"ANALY_DESY-ZN_XRD\": \"DE\", \"ANALY_UCSC\": \"US\", \"IN2P3-LPSC_VAC\": \"FR\", \"IHEP_PROD\": \"RU\", \"ZA-WITS-CORE\": \"IT\", \"IN2P3-CC_MCORE_HIMEM\": \"FR\", \"ANALY_CONNECT_SHORT\": \"US\", \"ANALY_CAM_SL6\": \"UK\", \"UKI-NORTHGRID-SHEF-HEP_MCORE\": \"UK\", \"ANALY_RALPP_SL6_GLEXEC\": \"UK\", \"ANALY_RRC-KI-HPC\": \"RU\", \"Microsoft-Azure\": \"CERN\", \"HEPHY-UIBK\": \"DE\", \"GRIF-LAL_HTCondor\": \"FR\", \"OU_OSCER_ATLAS_MCORE\": \"US\", \"ANALY_UTFSM\": \"ES\", \"WEIZMANN-LCG2_MCORE\": \"NL\", \"ANALY_TRIUMF\": \"CA\", \"ANALY_ROMANIA02_GLEXEC\": \"FR\", \"UKI-NORTHGRID-LIV-HEP_MCORE\": \"UK\", \"ANALY_OU_OCHEP_SWT2\": \"US\", \"CA-SCINET-T2\": \"CA\", \"ARC-TEST\": \"ND\", \"ANALY_OX_TEST\": \"UK\", \"ANALY_IN2P3-CC-T2_GLEXEC\": \"FR\", \"praguelcg2_TEST\": \"DE\", \"ANALY_DESY-ZN_GLEXEC\": \"DE\", \"ANALY_VICTORIA_GLEXEC\": \"CA\", \"MPPMU_MCORE\": \"DE\", \"UNIBE-LHEP_MCORE\": \"ND\", \"ANALY_UIO\": \"ND\", \"SLACXRD_MP8\": \"US\", \"LPSC_MCORE\": \"FR\", \"ANALY_TR-10-ULAKBIM\": \"NL\", \"ANALY_CPPM\": \"FR\", \"ANALY_ROMANIA07_GLEXEC\": \"FR\", \"OU_OSCER_ATLAS\": \"US\", \"ANALY_NERSC\": \"US\", \"ANALY_BNL_LOCAL\": \"US\", \"CERN-PROD-preprod\": \"CERN\", \"ORNL_Titan_MCORE\": \"US\", \"ANALY_BEIJING_GLEXEC\": \"FR\", \"Australia-ATLAS_MCORE\": \"CA\", \"ANALY_UNIBE-LHEP-UBELIX\": \"ND\", \"ANALY_RAL_SL6\": \"UK\", \"NERSC-PDSF\": \"OSG\", \"pic_HIMEM\": \"ES\", \"ANALY_MCGILL_TEST\": \"CA\", \"TRIUMF\": \"CA\", \"ANALY_UTFSM_GLEXEC\": \"ES\", \"ANALY_RHUL_SL6\": \"UK\", \"ANALY_HEPHY-UIBK_GLEXEC\": \"DE\", \"ANALY_BNL_CLOUD\": \"US\", \"ANALY_SHEF_SL6\": \"UK\", \"Taiwan-LCG2_MCORE\": \"TW\", \"TOKYO_HIMEM\": \"FR\", \"CYFRONET-LCG2_MCORE\": \"DE\", \"INFN-NAPOLI-RECAS-TEST\": \"IT\", \"ROMANIA16_MCORE\": \"FR\", \"ANALY_CERN_XROOTD\": \"CERN\", \"FMPhI-UNIBA_MCORE\": \"DE\", \"ANALY_FREIBURG\": \"DE\", \"OUHEP_OSG\": \"US\", \"UKI-LT2-IC-HEP_MCORE\": \"UK\", \"DESY-ZN\": \"DE\", \"INFN-ROMA1_MCORE\": \"IT\", \"ANALY_CPPM_GLEXEC\": \"FR\", \"CONNECT_ES\": \"US\", \"GRIF-LPNHE\": \"FR\", \"ANALY_BNL_Test_2_CE_1\": \"US\", \"ANALY_PSNC\": \"DE\", \"SLACXRD_LMEM\": \"US\", \"ANALY_BEIJING\": \"FR\", \"SiGNET_MCORE\": \"ND\", \"INFN-COSENZA-RECAS\": \"IT\", \"ANALY_LANCS_SL6\": \"UK\", \"RAL-LCG2_VHIMEM\": \"UK\", \"TRIUMF_TEST\": \"CA\", \"IN2P3-CC-T3_MCORE\": \"FR\", \"ANALY_MCGILL_GLEXEC\": \"CA\", \"ANALY_BNL_T3\": \"US\", \"INFN-FRASCATI_MCORE\": \"IT\", \"ZA-UJ\": \"IT\", \"CERN-PROD_CLOUD_MCORE\": \"CERN\", \"ANALY_INFN-COSENZA\": \"IT\", \"ANALY_IHEP\": \"RU\", \"LRZ-LMU_MUC_MCORE\": \"DE\", \"INFN-T1_HIMEM\": \"IT\", \"ANALY_ZA-UJ\": \"IT\", \"ANALY_TRIUMF_HIMEM\": \"CA\", \"DESY-HH\": \"DE\", \"ANALY_HEPHY-UIBK\": \"DE\", \"ANALY_WEIZMANN-CREAM\": \"NL\", \"SFU-LCG2_MCORE\": \"CA\", \"MWT2_MCORE\": \"US\", \"CPPM_MCORE\": \"FR\", \"UKI-SCOTGRID-GLASGOW_TEST\": \"UK\", \"UAM-LCG2_MCORE\": \"ES\", \"UNI-BONN\": \"DE\", \"UKI-NORTHGRID-LIV-HEP_SL6\": \"UK\", \"ANALY_NIKHEF-ELPROD\": \"NL\", \"ANALY_INFN-MILANO-ATLASC_GLEXEC\": \"IT\", \"ANALY_BHAM_SL6_GLEXEC\": \"UK\", \"UKI-SCOTGRID-DURHAM_MCORE\": \"UK\", \"ANALY_INFN-COSENZA-RECAS\": \"IT\", \"ANALY_ANLASC_Argo\": \"US\", \"GRIDPP_CLOUD\": \"UK\", \"ANALY_BNL_test\": \"US\", \"UKI-NORTHGRID-MAN-HEP_TEST\": \"UK\", \"BEIJING-PI_MCORE\": \"FR\", \"TW-FTT_MCORE\": \"TW\", \"ANALY_GLASGOW_SL6_GLEXEC\": \"UK\", \"SFU-LCG2\": \"CA\", \"RAL-LCG2_MCORE\": \"UK\", \"UKI-SOUTHGRID-CAM-HEP_SL6\": \"UK\", \"ANALY_BHAM_SL6\": \"UK\", \"ANALY_ANLASC_T3Test\": \"US\", \"ANALY_INFN-T1-VWN_TEST\": \"IT\", \"wuppertalprod_MCORE\": \"DE\", \"ANALY_HPC2N\": \"ND\", \"CERN-P1\": \"CERN\", \"praguelcg2\": \"DE\", \"BEIJING-TIANJIN-TH-1A_MCORE\": \"FR\", \"ANALY_CERN_TEST\": \"CERN\", \"AGLT2_SL6\": \"US\", \"ANALY_GR-01-AUTH_GLEXEC\": \"IT\", \"ANALY_CONNECT\": \"US\", \"UKI-NORTHGRID-MAN-HEP_OS_MCORE\": \"UK\", \"UKI-SOUTHGRID-OX-HEP\": \"UK\", \"WEIZMANN-LCG2\": \"NL\", \"ANALY_IN2P3-CC-T2\": \"FR\", \"ANALY_UTA_DANILA_TEST\": \"US\", \"ANALY_PIC_SL6_GLEXEC\": \"ES\", \"MPPMU\": \"DE\", \"AGLT2_MCORE\": \"US\", \"INFN-MILANO-ATLASC\": \"IT\", \"ANALY_GOEGRID\": \"DE\", \"ANALY_RRC-KI-T1\": \"RU\", \"LRZ-LMU_C2PAP\": \"DE\", \"ANALY_QMUL_SL6_GLEXEC\": \"UK\", \"UKI-NORTHGRID-LANCS-HEP_SL6\": \"UK\", \"UAM-LCG2\": \"ES\", \"CPPM\": \"FR\", \"IAAS_MCORE\": \"CA\", \"INFN-NAPOLI-ATLAS\": \"IT\", \"BEIJING\": \"FR\", \"ANALY_TEST-APF2\": \"US\", \"DESY-HH_TEST\": \"DE\", \"ANALY_RAL_SL6_GLEXEC\": \"UK\", \"ANALY_MANC_TEST\": \"UK\", \"CERN-P1_MCORE\": \"CERN\", \"Taiwan-LCG2\": \"TW\", \"FZK-LCG2_MCORE_HI\": \"DE\", \"ANALY_JINR_GLEXEC\": \"NL\", \"ES_ORNL_Titan\": \"US\", \"UNIBE-LHEP-UBELIX_MCORE\": \"ND\", \"FMPhI-UNIBA\": \"DE\", \"UKI-SOUTHGRID-OX-HEP_TEST\": \"UK\", \"ANALY_BNL_SE_Test\": \"US\", \"RAL-LCG2_SL6\": \"UK\", \"SLAC_PAUL_TEST\": \"US\", \"ARNES_MCORE\": \"ND\", \"ANALY_INFN-FRASCATI_GLEXEC\": \"IT\", \"GRIF-IRFU_MCORE\": \"FR\", \"RRC-KI-T1\": \"RU\", \"UKI-SOUTHGRID-OX-HEP_SL6\": \"UK\", \"OU_OCHEP_SWT2\": \"US\", \"BEIJING_MCORE\": \"FR\", \"NSC\": \"ND\", \"LRZ-LMU\": \"DE\", \"Lucille_CE\": \"US\", \"praguelcg2_MCORE\": \"DE\", \"IFIC_MCORE\": \"ES\", \"IHEP_MCORE\": \"RU\", \"TRIUMF_MCORE\": \"CA\", \"BNL_ITB_Test1\": \"OSG\", \"NCG-INGRID-PT_MCORE\": \"ES\", \"ANALY_TAIWAN_XROOTD_SL6\": \"TW\", \"ANALY_LBNL_TEST_CLOUD2\": \"US\", \"ANALY_LPSC_GLEXEC\": \"FR\", \"ITEP_MCORE\": \"NL\", \"CONNECT_MCORE\": \"US\", \"ANALY_INFN-NAPOLI\": \"IT\", \"BNL_EC2W2_MCORE\": \"US\", \"ANALY_UAM\": \"ES\", \"ANALY_INFN-T1_GLEXEC\": \"IT\", \"ANALY_ANLASC\": \"US\", \"ANALY_MPPMU_GLEXEC\": \"DE\", \"ANALY_TOKYO_GLEXEC\": \"FR\", \"FIAN_MCORE\": \"NL\", \"UNIBE-LHEP\": \"ND\", \"UNIBE-LHEP_CLOUD\": \"ND\", \"NIKHEF-ELPROD-HighMem\": \"NL\", \"ANALY_INFN-NAPOLI-RECAS\": \"IT\", \"ANALY_AGLT2_TEST_SL6-condor\": \"US\", \"UNI-SIEGEN-HEP\": \"DE\", \"CA-SCINET-T2_MCORE\": \"CA\", \"UKI-SCOTGRID-ECDF_SL6\": \"UK\", \"UIO\": \"ND\", \"ANALY_CERN_CLOUD\": \"CERN\", \"PSNC\": \"DE\", \"ANALY_INFN-ROMA1\": \"IT\", \"ANALY_INFN-ROMA3\": \"IT\", \"ANALY_INFN-ROMA2\": \"IT\", \"ANALY_FMPhI-UNIBA_GLEXEC\": \"DE\", \"BEIJING-ERA_MCORE\": \"FR\", \"LRZ-LMU_MCORE\": \"DE\", \"FIAN_PROD\": \"NL\", \"ANALY_MWT2_SL6\": \"US\", \"SFU-LCG2_ES\": \"CA\", \"UKI-NORTHGRID-LANCS-HEP_CLOUD\": \"UK\", \"NIKHEF-ELPROD\": \"NL\", \"ANALY_SARA\": \"NL\", \"EELA-UTFSM\": \"ES\", \"UMESHTEST\": \"US\", \"IL-TAU-HEP\": \"NL\", \"FZK-LCG2_MCORE_LO\": \"DE\", \"OU_OSCER_ATLAS_OPP\": \"US\", \"Nebraska_SBGRID\": \"OSG\", \"ANALY_IFAE_GLEXEC\": \"ES\", \"MPPMU-HYDRA_MCORE\": \"DE\", \"SLACXRD\": \"US\", \"Harvard-Exp_SBGRID\": \"OSG\", \"Arizona_ES\": \"US\", \"Lucille_MCORE\": \"US\", \"ANALY_NIKHEF-ELPROD_SHORT\": \"NL\", \"ROMANIA07_ARC\": \"FR\", \"UTA_SWT2\": \"US\", \"JINR_PROD\": \"NL\", \"NERSC_Cori\": \"US\", \"ANALY_SFU\": \"CA\", \"ANALY_LRZ_TEST\": \"DE\", \"ANALY_SCINET\": \"CA\", \"UW_VDT_ITB\": \"OSG\", \"IN2P3-CC_VVL\": \"FR\", \"ANALY_CERNVM_test\": \"CERN\", \"CSCS-LCG2_MCORE\": \"DE\", \"BOINC\": \"CERN\", \"ANALY_SCINET_GLEXEC\": \"CA\", \"ANALY_SFU_GLEXEC\": \"CA\", \"CONNECT\": \"US\", \"ANALY_INFN-FRASCATI_PODtest\": \"IT\", \"UTA_DANILA_TEST\": \"US\", \"IN2P3-CC-T2\": \"FR\", \"SARA-MATRIX_MCORE\": \"NL\", \"UKI-LT2-IC-HEP_TEST\": \"UK\", \"CERN-PROD_SHORT\": \"CERN\", \"ANALY_SHEF_SL6_GLEXEC\": \"UK\", \"IN2P3-CC\": \"FR\", \"ATLAS_OPP_OSG\": \"US\", \"ANALY_SARA_hpc_cloud_xrootd\": \"NL\", \"ANALY_FZK_GLEXEC\": \"DE\", \"BNL_EC2E1\": \"US\", \"ANALY_GRIF-LPNHE\": \"FR\", \"UKI-SOUTHGRID-SUSX_MCORE\": \"UK\", \"Harvard-East_SBGRID\": \"OSG\", \"IAAS\": \"CA\", \"UNI-FREIBURG\": \"DE\", \"TOKYO_MCORE\": \"FR\", \"SLAC_ES\": \"US\", \"ANALY_AGLT2_SL6\": \"US\", \"ANALY_SARA_hpc_cloud_rfio\": \"NL\", \"UKI-SCOTGRID-GLASGOW_MCORE\": \"UK\", \"Taiwan-LCG2_VL\": \"TW\", \"UNIGE-DPNC\": \"ND\", \"Firefly_SBGRID\": \"OSG\", \"FZK-LCG2_HIMEM\": \"DE\", \"ANALY_GLASGOW_SL6\": \"UK\", \"UNI-DORTMUND\": \"DE\", \"NIKHEF-ELPROD_MCORE\": \"NL\", \"ANALY_CYF\": \"DE\", \"ANALY_BNL_MCORE\": \"US\", \"HU_ATLAS_Tier2\": \"US\", \"UKI-SOUTHGRID-SUSX_SL6\": \"UK\", \"UKI-LT2-RHUL_SL6\": \"UK\", \"ANALY_VICTORIA\": \"CA\", \"UKI-SOUTHGRID-CAM-HEP_MCORE\": \"UK\", \"ANALY_BU_ATLAS_Tier2_SL6\": \"US\", \"UKI-NORTHGRID-LANCS-HEP_MCORE\": \"UK\", \"RAL-LCG2_VAC\": \"UK\", \"ANALY_LAPP_GLEXEC\": \"FR\", \"CERN-PROD_MCORE\": \"CERN\", \"ANALY_INFN-T1-VWN2_TEST\": \"IT\", \"ANALY_SUSX_SL6\": \"UK\", \"INFN-LECCE\": \"IT\", \"UNI-FREIBURG_MCORE\": \"DE\", \"LUNARC_MCORE\": \"ND\", \"UNIBE-LHEP-UBELIX\": \"ND\", \"ANALY_INFN-ROMA1_GLEXEC\": \"IT\", \"ANALY_HU_ATLAS_Tier2\": \"US\", \"LAPP\": \"FR\", \"ANALY_NCG-INGRID-PT_SL6\": \"ES\", \"Microsoft-Azure_MCORE\": \"CERN\", \"ANALY_INFN-T1\": \"IT\", \"INFN-NAPOLI-RECAS_MCORE\": \"IT\", \"ANALY_QMUL_SL6\": \"UK\", \"pic\": \"ES\", \"UKI-NORTHGRID-MAN-HEP_MCORE\": \"UK\", \"GoeGrid\": \"DE\", \"ANALY_IEPSAS-Kosice\": \"DE\", \"CONNECT_CLOUD\": \"US\", \"INFN-T1_TEST\": \"IT\", \"ANALY_DESY-ZN\": \"DE\", \"ANALY_MWT2_MCORE\": \"US\", \"ANALY_LIV_SL6\": \"UK\", \"CSCS-LCG2\": \"DE\", \"ANALY_GRIF-LAL_HTCondor\": \"FR\", \"ANALY_IAAS_TEST\": \"CA\", \"GRIF-LPNHE_MCORE\": \"FR\", \"UTA_SWT2_MCORE\": \"US\", \"UKI-LT2-QMUL_TEST\": \"UK\", \"ANLASC\": \"US\", \"ANALY_IFIC_GLEXEC\": \"ES\", \"ANALY_LPC\": \"FR\", \"RRC-KI_PROD\": \"NL\", \"ANALY_LIV_SL6_GLEXEC\": \"UK\", \"BNL_SITE_GK02\": \"OSG\", \"TR-10-ULAKBIM\": \"NL\", \"AM-04-YERPHI\": \"NL\", \"ANALY_IN2P3-CC\": \"FR\", \"DBCE_MCORE_preprod\": \"CERN\", \"ROMANIA16\": \"FR\", \"ROMANIA14\": \"FR\", \"INFN-NAPOLI-RECAS\": \"IT\", \"DESY-HH_MCORE\": \"DE\", \"TUDresden-ZIH\": \"DE\", \"ANALY_CERN_SLC6\": \"CERN\", \"GR-01-AUTH\": \"IT\", \"ANALY_wuppertalprod\": \"DE\", \"LPC_MCORE\": \"FR\", \"BNL_LOCAL\": \"US\"}";


}
