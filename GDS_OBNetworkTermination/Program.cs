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
                #region 1. Get & Open Connection
                
                //"Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi"
                string connStr = args[0];
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                #endregion

                #region 2. Call Procedure

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

                #region 3. Process

                #region 3.1. Find list of segments as reference

                OleDbCommand cmd_Segment = new OleDbCommand();
                cmd_Segment.Connection = conn;
                cmd_Segment.CommandText = "SELECT DISTINCT GDS_SEGMENT FROM REF_BI_GDS_SEGMENT";
                cmd_Segment.CommandType = CommandType.Text;
                OleDbDataReader dr_Segment = cmd_Segment.ExecuteReader();

                #endregion

                #region 3.2. Starts referencing data from segments

                while (dr_Segment.Read())
                {
                    string segment = dr_Segment.GetString(0);

                    #region 3.2.2. Referencing data from main table

                    OleDbCommand cmd_SegmentDetails = new OleDbCommand();
                    cmd_SegmentDetails.Connection = conn;
                    cmd_SegmentDetails.CommandText = "select ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_IPID, FEAT_CODE, PDDP_IPID, PDP_IPID, CAB_IPID, RT_TYPE, FDP_CLASS, FDC_IPID, FDC_CODE, ADDRESS, COOR_X, COOR_Y, NETWORK_ID, ROWID from BI_NET_TMNT WHERE SEGMENT = :sgm AND BI_BATCH_ID is null";
                    cmd_SegmentDetails.Parameters.AddWithValue(":sgm", segment);
                    cmd_SegmentDetails.CommandType = CommandType.Text;
                    OleDbDataReader dr_SegmentDetails = cmd_SegmentDetails.ExecuteReader();

                    #endregion

                    #region 3.2.3 Starts processing if main segment has related value in main table

                    if (dr_SegmentDetails.HasRows)
                    {
                        List<string> lines = new List<string>();                        

                        #region 3.2.2.1 Get Batch ID

                        OleDbCommand cmd_GetBID = new OleDbCommand();
                        cmd_GetBID.Connection = conn;
                        cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                        cmd_GetBID.CommandType = CommandType.Text;
                        OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();
                        
                        dr_BID.Read();
                        string bid = dr_BID.GetDecimal(0).ToString();
                        dr_BID.Close();
                        cmd_GetBID.Dispose();

                        #endregion

                        #region 3.2.2.2 Record StartTime

                        OleDbCommand cmd_SetStartTime = new OleDbCommand();
                        cmd_SetStartTime.Connection = conn;
                        cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_NetworkTermination', 'EdgeFrontier.GDS.OBNetworkTermination', SysDate, 'GDS', 'OUTBOUND', 0)";
                        cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetStartTime.CommandType = CommandType.Text;
                        cmd_SetStartTime.ExecuteNonQuery();
                        cmd_SetStartTime.Dispose();

                        #endregion

                        #region 3.2.2.3 Starts reading and processing in details

                        while (dr_SegmentDetails.Read())
                        {
                            //Store data from executed query into variables
                            string ACTION_TYPE = (!dr_SegmentDetails.IsDBNull(0)) ? dr_SegmentDetails.GetString(0).Trim() : "";
                            string FEAT_TYPE = (!dr_SegmentDetails.IsDBNull(1)) ? dr_SegmentDetails.GetString(1).Trim() : "";
                            string EXC_ABB = (!dr_SegmentDetails.IsDBNull(2)) ? dr_SegmentDetails.GetString(2).Trim() : "";
                            string IPID = (!dr_SegmentDetails.IsDBNull(3)) ? dr_SegmentDetails.GetDecimal(3).ToString().Trim() : "";
                            string BND_IPID = (!dr_SegmentDetails.IsDBNull(4)) ? dr_SegmentDetails.GetDecimal(4).ToString().Trim() : "";
                            string FEAT_CODE = (!dr_SegmentDetails.IsDBNull(5)) ? dr_SegmentDetails.GetString(5).Trim() : "";
                            string PDDP_IPID = (!dr_SegmentDetails.IsDBNull(6)) ? dr_SegmentDetails.GetDecimal(6).ToString().Trim() : "";
                            string PDP_IPID = (!dr_SegmentDetails.IsDBNull(7)) ? dr_SegmentDetails.GetDecimal(7).ToString().Trim() : "";
                            string CAB_IPID = (!dr_SegmentDetails.IsDBNull(8)) ? dr_SegmentDetails.GetDecimal(8).ToString().Trim() : "";
                            string RT_TYPE = (!dr_SegmentDetails.IsDBNull(9)) ? dr_SegmentDetails.GetString(9).Trim() : "";
                            string FDP_CLASS = (!dr_SegmentDetails.IsDBNull(10)) ? dr_SegmentDetails.GetString(10).Trim() : "";
                            string FDC_IPID = (!dr_SegmentDetails.IsDBNull(11)) ? dr_SegmentDetails.GetDecimal(11).ToString().Trim() : "";
                            string FDC_CODE = (!dr_SegmentDetails.IsDBNull(12)) ? dr_SegmentDetails.GetString(12).Trim() : "";
                            string ADDRESS = (!dr_SegmentDetails.IsDBNull(13)) ? dr_SegmentDetails.GetString(13).Trim() : "";
                            string COOR_X = (!dr_SegmentDetails.IsDBNull(14)) ? dr_SegmentDetails.GetDecimal(14).ToString().Trim() : "";
                            string COOR_Y = (!dr_SegmentDetails.IsDBNull(15)) ? dr_SegmentDetails.GetDecimal(15).ToString().Trim() : "";
                            string NETWORK_ID = (!dr_SegmentDetails.IsDBNull(16)) ? dr_SegmentDetails.GetDecimal(16).ToString().Trim() : "";
                            string ROWID = dr_SegmentDetails.GetString(17);
                            
                            //Prepare first part of line
                            string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}|{14}|{15}|", ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_IPID, FEAT_CODE, PDDP_IPID, PDP_IPID, CAB_IPID, RT_TYPE, FDP_CLASS, FDC_IPID, FDC_CODE, ADDRESS, COOR_X, COOR_Y);

                            #region 3.2.2.3.1 Referencing data from Child table

                            OleDbCommand cmd_CMPP = new OleDbCommand();
                            cmd_CMPP.Connection = conn;
                            cmd_CMPP.CommandText = "select CMPP_ID, CMPP_VALUE, ROWID from BI_NET_CMPP where NETWORK_ID = :netid ORDER BY CMPP_ID ASC";
                            cmd_CMPP.Parameters.AddWithValue(":netid", NETWORK_ID);
                            cmd_CMPP.CommandType = CommandType.Text;
                            OleDbDataReader dr_CMPP = cmd_CMPP.ExecuteReader();

                            #endregion

                            #region 3.2.2.3.2 Starts processing if parent table has related values in child table

                            if (dr_CMPP.HasRows)
                            {
                                ArrayList cmpp_list = new ArrayList();

                                #region 3.2.2.3.2.1 Starts referencing data from parent

                                while (dr_CMPP.Read())
                                {
                                    //string CMPP_ID = dr_Coor.GetDecimal(0).ToString();
                                    string CMPP_VALUE = dr_CMPP.GetString(1);
                                    string ROWID_CHILD = dr_CMPP.GetString(2);

                                    //Add data from the child table to be appended to the current line
                                    cmpp_list.Add(CMPP_VALUE);

                                    #region Update child table

                                    OleDbCommand cmd_GetBIOChild = new OleDbCommand();
                                    cmd_GetBIOChild.Connection = conn;
                                    cmd_GetBIOChild.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_NET_CMPP WHERE ROWNUM = 1";
                                    cmd_GetBIOChild.CommandType = CommandType.Text;
                                    OleDbDataReader dr_BIOChild = cmd_GetBIOChild.ExecuteReader();

                                    dr_BIOChild.Read();
                                    string bioChild = dr_BIOChild.GetDecimal(0).ToString();
                                    dr_BIOChild.Close();
                                    cmd_GetBIOChild.Dispose();

                                    OleDbCommand cmd_UpdateCoor = new OleDbCommand();
                                    cmd_UpdateCoor.Connection = conn;
                                    cmd_UpdateCoor.CommandText = "UPDATE BI_NET_CMPP SET BI_INSERT_ORDER = :bio WHERE ROWID = :rid";
                                    cmd_UpdateCoor.Parameters.AddWithValue(":bio", bioChild);
                                    cmd_UpdateCoor.Parameters.AddWithValue(":rid", ROWID_CHILD);
                                    cmd_UpdateCoor.ExecuteNonQuery();
                                    cmd_UpdateCoor.Dispose();

                                    #endregion
                                }

                                #endregion

                                #region 3.2.2.3.2.2 Append list of data from child table to the current line

                                int size = cmpp_list.Count;

                                for (int i = 0; i < size; ++i)
                                {
                                    line += (i == 0 ? "" : "|") + (string)cmpp_list[i];
                                }

                                #endregion
                            }

                            #endregion

                            #region 3.2.2.3.3 Close child table reader and dispose cursor

                            dr_CMPP.Close();
                            cmd_CMPP.Dispose();

                            #endregion

                            if (line.EndsWith("|")) line.TrimEnd('|');

                            //Add line to list of output lines
                            lines.Add(line);

                            #region 3.2.2.3.4 Update main table

                            OleDbCommand cmd_GetBIO = new OleDbCommand();
                            cmd_GetBIO.Connection = conn;
                            cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_NET_TMNT WHERE ROWNUM = 1";
                            cmd_GetBIO.CommandType = CommandType.Text;
                            OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                            dr_BIO.Read();
                            string bio = dr_BIO.GetDecimal(0).ToString();
                            dr_BIO.Close();
                            cmd_GetBIO.Dispose();

                            OleDbCommand cmd_UpdateMain = new OleDbCommand();
                            cmd_UpdateMain.Connection = conn;
                            cmd_UpdateMain.CommandText = "UPDATE BI_NET_TMNT set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                            cmd_UpdateMain.Parameters.AddWithValue(":bid_val", bid);
                            cmd_UpdateMain.Parameters.AddWithValue(":bio_val", bio);
                            cmd_UpdateMain.Parameters.AddWithValue(":rowid_val", ROWID);
                            cmd_UpdateMain.ExecuteNonQuery();
                            cmd_UpdateMain.Dispose();

                            #endregion
                        }

                        #endregion

                        #region 3.2.2.4 Write CSV file

                        string date = DateTime.Now.ToString("yyyyMMdd");
                        string filename = segment + "_DailyTermination_" + date + ".csv";

                        if (File.Exists(filename))
                        {
                            File.Delete(filename);
                        }

                        //Write data to file
                        File.AppendAllLines(filename, lines);

                        #endregion

                        #region 3.2.2.5 Record EndTime

                        OleDbCommand cmd_SetEndTime = new OleDbCommand();
                        cmd_SetEndTime.Connection = conn;
                        cmd_SetEndTime.CommandText = "UPDATE BI_BATCH SET TIME_END = SysDate, FILENAME = :filename WHERE BATCH_ID = :bid";
                        cmd_SetEndTime.Parameters.AddWithValue(":filename", filename);
                        cmd_SetEndTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetEndTime.CommandType = CommandType.Text;
                        cmd_SetEndTime.ExecuteNonQuery();
                        cmd_SetEndTime.Dispose();

                        #endregion
                    }

                    #endregion

                    #region 3.2.4. Close main table reader and dispose cursor

                    dr_SegmentDetails.Close();
                    cmd_SegmentDetails.Dispose();

                    #endregion
                }

                #endregion

                #endregion

                #region 4. Close Connection

                dr_Segment.Close();
                cmd_Segment.Dispose();
                conn.Dispose();
                conn.Close();

                #endregion
            }
            else
            {
                Console.WriteLine("Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"");
            }
        }
    }
}
