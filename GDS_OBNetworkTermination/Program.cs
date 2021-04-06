using System;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Collections;
using System.Collections.Generic;

namespace GDS_OBNetworkTermination
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                //args[0]
                //"Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi"
                string connStr = "Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi";
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                #region Procedure

                OleDbCommand cmd_callProcDisable = new OleDbCommand("NEPS.DISABLE_ALL_TRIGGER", conn);
                cmd_callProcDisable.CommandType = CommandType.StoredProcedure;
                cmd_callProcDisable.ExecuteNonQuery();
                cmd_callProcDisable.Dispose();

                OleDbCommand cmd_callProcedure = new OleDbCommand("GDS_OUTBOUND_NT_TMNT", conn);
                cmd_callProcedure.CommandType = CommandType.StoredProcedure;
                cmd_callProcedure.ExecuteNonQuery();
                cmd_callProcedure.Dispose();

                OleDbCommand cmd_callProcEnable = new OleDbCommand("NEPS.ENABLE_ALL_TRIGGER", conn);
                cmd_callProcEnable.CommandType = CommandType.StoredProcedure;
                cmd_callProcEnable.ExecuteNonQuery();
                cmd_callProcEnable.Dispose();

                #endregion

                OleDbCommand cmd_Segment = new OleDbCommand();
                cmd_Segment.Connection = conn;
                cmd_Segment.CommandText = "SELECT DISTINCT GDS_SEGMENT FROM REF_BI_GDS_SEGMENT";
                cmd_Segment.CommandType = CommandType.Text;
                OleDbDataReader dr_Segment = cmd_Segment.ExecuteReader();

                //int i = 0;

                while (dr_Segment.Read())
                {
                    string segment = dr_Segment.GetString(0);

                    //Console.WriteLine("{0})\t {1}", ++i, segment);
                    OleDbCommand cmd_SegmentDetails = new OleDbCommand();
                    cmd_SegmentDetails.Connection = conn;
                    cmd_SegmentDetails.CommandText = "select ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_IPID, FEAT_CODE, PDDP_IPID, PDP_IPID, CAB_IPID, RT_TYPE, FDP_CLASS, FDC_IPID, FDC_CODE, ADDRESS, COOR_X, COOR_Y, NETWORK_ID, ROWID from BI_NET_TMNT WHERE SEGMENT = :sgm AND BI_BATCH_ID is null";
                    cmd_SegmentDetails.Parameters.AddWithValue(":sgm", segment);
                    cmd_SegmentDetails.CommandType = CommandType.Text;
                    OleDbDataReader dr_SegmentDetails = cmd_SegmentDetails.ExecuteReader();

                    if (dr_SegmentDetails.HasRows)
                    {
                        List<string> lines = new List<string>();

                        #region StartTime

                        OleDbCommand cmd_GetBID = new OleDbCommand();
                        cmd_GetBID.Connection = conn;
                        cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                        cmd_GetBID.CommandType = CommandType.Text;
                        OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();

                        //get batch id
                        dr_BID.Read();
                        string bid = dr_BID.GetDecimal(0).ToString();
                        dr_BID.Close();
                        cmd_GetBID.Dispose();

                        OleDbCommand cmd_SetStartTime = new OleDbCommand();
                        cmd_SetStartTime.Connection = conn;
                        cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_DeletedNetworkTermination', 'EdgeFrontier.GDS.OBDeletedNetworkTermination', SysDate, 'GDS', 'OUTBOUND', 0)";
                        cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetStartTime.CommandType = CommandType.Text;
                        cmd_SetStartTime.ExecuteNonQuery();
                        cmd_SetStartTime.Dispose();
                        
                        #endregion

                        while (dr_SegmentDetails.Read())
                        {
                            string ACTION_TYPE = dr_SegmentDetails.GetString(0);
                            string FEAT_TYPE = dr_SegmentDetails.GetString(1);                            
                            string EXC_ABB = dr_SegmentDetails.GetString(2);
                            string IPID = dr_SegmentDetails.GetDecimal(3).ToString();
                            string BND_IPID = dr_SegmentDetails.GetDecimal(4).ToString();
                            string FEAT_CODE = dr_SegmentDetails.GetString(5);
                            string PDDP_IPID = dr_SegmentDetails.GetDecimal(6).ToString();
                            string PDP_IPID = dr_SegmentDetails.GetDecimal(7).ToString();
                            string CAB_IPID = dr_SegmentDetails.GetDecimal(8).ToString();
                            string RT_TYPE = dr_SegmentDetails.GetString(9);
                            string FDP_CLASS = dr_SegmentDetails.GetString(10);
                            string FDC_IPID = dr_SegmentDetails.GetDecimal(11).ToString();
                            string FDC_CODE = dr_SegmentDetails.GetString(12);
                            string ADDRESS = dr_SegmentDetails.GetString(13);
                            string COOR_X = dr_SegmentDetails.GetDecimal(14).ToString();
                            string COOR_Y = dr_SegmentDetails.GetDecimal(15).ToString();
                            string NETWORK_ID = dr_SegmentDetails.GetDecimal(16).ToString();
                            string ROWID = dr_SegmentDetails.GetString(17);
                            
                            string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}|{14}|{15}|", ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_IPID, FEAT_CODE, PDDP_IPID, PDP_IPID, CAB_IPID, RT_TYPE, FDP_CLASS, FDC_IPID, FDC_CODE, ADDRESS, COOR_X, COOR_Y);

                            OleDbCommand cmd_CMPP = new OleDbCommand();
                            cmd_CMPP.Connection = conn;
                            cmd_CMPP.CommandText = "select CMPP_ID, CMPP_VALUE, ROWID from BI_NET_CMPP where NETWORK_ID = :netid ORDER BY CMPP_ID ASC";
                            cmd_CMPP.Parameters.AddWithValue(":netid", NETWORK_ID);
                            cmd_CMPP.CommandType = CommandType.Text;
                            OleDbDataReader dr_CMPP = cmd_CMPP.ExecuteReader();

                            if (dr_CMPP.HasRows)
                            {
                                ArrayList cmpp_list = new ArrayList();

                                while (dr_CMPP.Read())
                                {
                                    //string CMPP_ID = dr_Coor.GetDecimal(0).ToString();
                                    string CMPP_VALUE = dr_CMPP.GetDecimal(1).ToString();
                                    string ROWID_CHILD = dr_CMPP.GetString(2);

                                    cmpp_list.Add(CMPP_VALUE);

                                    #region UpdateChild

                                    OleDbCommand cmd_GetBIOChild = new OleDbCommand();
                                    cmd_GetBIOChild.Connection = conn;
                                    cmd_GetBIOChild.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_DEL_NET_TMNT WHERE ROWNUM = 1";
                                    cmd_GetBIOChild.CommandType = CommandType.Text;
                                    OleDbDataReader dr_BIOChild = cmd_GetBIOChild.ExecuteReader();

                                    dr_BIOChild.Read();
                                    string bioChild = dr_BIOChild.GetDecimal(0).ToString();
                                    dr_BIOChild.Close();
                                    cmd_GetBIOChild.Dispose();

                                    OleDbCommand cmd_UpdateCoor = new OleDbCommand();
                                    cmd_UpdateCoor.Connection = conn;
                                    cmd_UpdateCoor.CommandText = "UPDATE BI_NET_CMPP SET BI_INSERT_ORDER = :bio WHERE ROWID = :rowid";
                                    cmd_UpdateCoor.Parameters.AddWithValue(":bio", bioChild);
                                    cmd_UpdateCoor.Parameters.AddWithValue(":rowid", ROWID_CHILD);
                                    cmd_UpdateCoor.ExecuteNonQuery();
                                    cmd_UpdateCoor.Dispose();

                                    #endregion
                                }

                                int size = cmpp_list.Count;

                                for (int i = 0; i < size; ++i)
                                {
                                    line += (i == 0 ? "" : "|") + (string)cmpp_list[i];
                                }
                            }

                            dr_CMPP.Close();
                            cmd_CMPP.Dispose();

                            lines.Add(line);

                            #region UpdateParent

                            OleDbCommand cmd_GetBIO = new OleDbCommand();
                            cmd_GetBIO.Connection = conn;
                            cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_NET_TMNT WHERE ROWNUM = 1";
                            cmd_GetBIO.CommandType = CommandType.Text;
                            OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                            dr_BIO.Read();
                            string bio = dr_BIO.GetDecimal(0).ToString();
                            dr_BIO.Close();
                            cmd_GetBIO.Dispose();

                            OleDbCommand cmd_UpdateDeletion = new OleDbCommand();
                            cmd_UpdateDeletion.Connection = conn;
                            cmd_UpdateDeletion.CommandText = "UPDATE BI_NET_TMNT set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                            cmd_UpdateDeletion.Parameters.AddWithValue(":bid_val", bid);
                            cmd_UpdateDeletion.Parameters.AddWithValue(":bio_val", bio);
                            cmd_UpdateDeletion.Parameters.AddWithValue(":rowid_val", ROWID);
                            cmd_UpdateDeletion.ExecuteNonQuery();
                            cmd_UpdateDeletion.Dispose();

                            #endregion
                        }

                        string date = DateTime.Now.ToString("yyyyMMdd");
                        string filename = segment + "_DailyTermination_" + date + ".csv";

                        if (File.Exists(filename))
                        {
                            File.Delete(filename);
                        }

                        File.AppendAllLines(filename, lines);

                        OleDbCommand cmd_SetEndTime = new OleDbCommand();
                        cmd_SetEndTime.Connection = conn;
                        cmd_SetEndTime.CommandText = "UPDATE BI_BATCH SET TIME_END = SysDate, FILENAME = :filename WHERE BATCH_ID = :bid";
                        cmd_SetEndTime.Parameters.AddWithValue(":filename", filename);
                        cmd_SetEndTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetEndTime.CommandType = CommandType.Text;
                        cmd_SetEndTime.ExecuteNonQuery();
                        cmd_SetEndTime.Dispose();
                    }

                    dr_SegmentDetails.Close();
                    cmd_SegmentDetails.Dispose();
                }

                dr_Segment.Close();
                cmd_Segment.Dispose();
                conn.Dispose();
                conn.Close();
            }
            else
            {
                Console.WriteLine("Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"");
            }
        }
    }
}
