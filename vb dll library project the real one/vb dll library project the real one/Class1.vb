Imports System.Data
Imports System.IO
'Imports ITServeLib.Entities.Sync
'Imports ITServeLib.Entities.ITDB

Public Class GraniteConvert
    Public Structure ITassets
        Dim ML_ID As Int32
        Dim Asset_Use As String
        Dim City As String
        Dim Drainage_Area As String
        Dim DS_MH As String
        Dim DS_GradetoInvert As Double
        Dim DS_RimtoGrade As Double
        Dim DS_RimtoInvert As Double
        Dim Joint_Length As Double
        Dim Lining As String
        Dim Location As String
        Dim Location_Details As String
        Dim Map_1 As String
        Dim Material As String
        Dim ML_Name As String
        Dim Owner As String
        Dim Pipe_Height As Int32
        Dim Pipe_Shape As String
        Dim Pipe_Width As Int32
        Dim Section_Length As Double
        Dim Sewer_Category As String
        Dim Street As String
        Dim US_MH As String
        Dim US_RimtoInvert As Single
        Dim US_GradetoInvert As Single
        Dim US_RimtoGrade As Single
        Dim Year_Constructed As Int32
        Dim Year_Renewed As Int32
        Dim Merge_GUID As String
    End Structure
    Public Structure ITinspections
        Dim ML_ID As Int32
        Dim MLI_ID As Int32
        Dim Sheet_Number As Int32
        Dim Certificate_Number As String
        Dim Customer As String
        Dim Cleaned As String
        Dim Inspection_Date As DateTime
        Dim Start_Time As DateTime
        Dim Last_Modified_Date As DateTime
        Dim Clean_Date As DateTime
        Dim Creation_Date As DateTime
        Dim Inspection_Direction As String
        Dim Media_Number As String
        Dim Flow_Control As String
        Dim TVOperator As String
        Dim PACP_Custom_1 As String
        Dim PACP_Custom_2 As String
        Dim PACP_Custom_3 As String
        Dim PACP_Custom_4 As String
        Dim PACP_Custom_5 As String
        Dim Reason_of_Inspection As String
        Dim Reverse_Setup As Int32
        Dim Weather As String
        Dim WO_Number As String
        Dim PO_Number As String
        Dim Inspected_Length As Single
        Dim Additional_Info As String
        Dim Merge_GUID As String
        Dim Parent_GUID As String
        Dim VideoMedia As Integer
    End Structure
    Public Structure ITobs
        Dim MLI_ID As Int32
        Dim MLO_ID As Int32
        Dim Clock_From As Int32
        Dim Clock_To As Int32
        Dim Value_1st_Dimension As Int32
        Dim Value_2nd_Dimension As Int32
        Dim Value_Percent As Int32
        Dim Code As String
        Dim Distance As Single
        Dim Remarks As String
        Dim Continuous As String
        Dim Joint As Boolean
        Dim IsReverse As Boolean
        Dim Grade As Int32
        Dim Observation_Text As String
        Dim Digital_Time As String
        Dim Merge_GUID As String
        Dim Parent_GUID As String
        Dim obsMedia As String
    End Structure
    Public Structure ITmedia
        Dim Media_ID As Int32
        Dim File_Name As String
        Dim CRC As String
        Dim File_Path As String
        Dim File_Type As String
        Dim SizeMB As Single
        Dim Media_Path_ID As Int32
        Dim Merge_GUID As String
        Dim Parent_GUID As String
    End Structure
    Dim dtCodes As New DataTable
    Dim dtHoles As New DataTable
    Public Sub grabGranite(ByVal conSource As OleDb.OleDbConnection, ByVal conTarget As OleDb.OleDbConnection)
        Dim setSource As New DataSet
        Dim adaSource As New OleDb.OleDbDataAdapter(conSource.CreateCommand)   'ITServeLib.DBConnectionFactory.CreateDBAdapter(conSourceType)
        Dim v As Integer = getVersion(conSource)

        If v = 17 Then
            adaSource.SelectCommand.CommandText = "SELECT * FROM TVInspection;"
            adaSource.Fill(setSource, "Inspections")
            adaSource.SelectCommand.CommandText = "SELECT * FROM VideoMedia;"
            adaSource.Fill(setSource, "Videos")
            adaSource.SelectCommand.CommandText = "SELECT * FROM MediaCatalog;"
            adaSource.Fill(setSource, "Media_Path")
            adaSource.SelectCommand.CommandText = "SELECT `__Key` as KeyID, Code, Description FROM Code;"
            adaSource.Fill(dtCodes)
            adaSource.SelectCommand.CommandText = "SELECT `__Key` as KeyID, ManholeID FROM Manhole;"
            adaSource.Fill(dtHoles)
        ElseIf v >= 28 Then
            adaSource.SelectCommand.CommandText = "SELECT * FROM Main_Inspection;"
            adaSource.Fill(setSource, "Inspections")
            adaSource.SelectCommand.CommandText = "SELECT * FROM Video_Media;"
            adaSource.Fill(setSource, "Videos")
            adaSource.SelectCommand.CommandText = "SELECT * FROM Media_Catalog;"
            adaSource.Fill(setSource, "Media_Path")
            adaSource.SelectCommand.CommandText = "SELECT `Key` as KeyID, Code, Description FROM Code;"
            adaSource.Fill(dtCodes)
            adaSource.SelectCommand.CommandText = "SELECT `Key` as KeyID, MANHOLE_ID AS ManholeID FROM Manhole;"
            adaSource.Fill(dtHoles)
            adaSource.SelectCommand.CommandText = "SELECT CONTAINERKEY, OBSERVATION__PHOTOS.KEY AS KeyID, FULL_PATH FROM OBSERVATION__PHOTOS LEFT JOIN PHOTO ON OBSERVATION__PHOTOS.KEY=PHOTO.KEY;"
            adaSource.Fill(setSource, "Photos")

        Else
            'MsgBox("Uknown DB version - aborting conversion.")
        End If

        'adaSource.SelectCommand = conSource.CreateCommand
        adaSource.SelectCommand.CommandText = "SELECT * FROM ASSET;"
        adaSource.Fill(setSource, "Assets")

        adaSource.SelectCommand.CommandText = "SELECT * FROM Observation;"
        adaSource.Fill(setSource, "Observations")

        Dim assets As New List(Of ITassets)
        Dim inspections As New List(Of ITinspections)
        Dim observations As New List(Of ITobs)
        Dim media As New List(Of ITmedia)
        Dim obsMedia As New List(Of ITmedia)

        If v = 17 Then
            assets = MLConvert17(setSource.Tables("Assets"))
            inspections = MLIConvert17(setSource.Tables("Inspections"), assets)
            observations = MLOConvert17(setSource.Tables("Observations"), inspections)
            media = MediaConvert17(setSource.Tables("Videos"), inspections)
        ElseIf v >= 28 Then
            assets = MLConvert35(setSource.Tables("Assets"))
            inspections = MLIConvert35(setSource.Tables("Inspections"), assets)
            observations = MLOConvert35(setSource.Tables("Observations"), inspections)
            media = MediaConvert35(setSource.Tables("Videos"), inspections)
            obsMedia = obsMediaConvert35(setSource.Tables("Photos"), observations)
        End If

        Dim cmd As OleDb.OleDbCommand = conTarget.CreateCommand
        writeA(cmd, assets)
        cmd.Parameters.Clear()
        writeI(cmd, inspections)
        cmd.Parameters.Clear()
        writeO(cmd, observations)
        cmd.Parameters.Clear()
        writeM(cmd, media)
        If obsMedia.Count > 0 Then
            writeM(cmd, obsMedia, True)
        End If
        DowndateProjectTable(conTarget, "ML")
        DowndateProjectTable(conTarget, "MLI")
        DowndateProjectTable(conTarget, "MLO")
        DowndateProjectTable(conTarget, "Media")
        'MsgBox("Convert Complete")
    End Sub

    Private Sub writeM(ByVal cmd As OleDb.OleDbCommand, ByVal med As List(Of ITmedia), Optional ByVal isObs As Boolean = False)
        cmd.Parameters.AddRange(parametersM(cmd))
        UpdateProjectTable(cmd.Connection, "Media")
        cmd.CommandText = "INSERT INTO Media (File_Name, CRC, File_Path, File_Type, SizeMB, Media_Path_ID, Merge_GUID, Parent_GUID) VALUES (?, ?, ?, ?, ?, Media_Path_ID, ?, ?)"
        For Each item In med
            cmd.Parameters(0).Value = nothingToDBNull(item.File_Name)
            cmd.Parameters(1).Value = nothingToDBNull(item.CRC)
            cmd.Parameters(2).Value = nothingToDBNull(item.File_Path)
            cmd.Parameters(3).Value = nothingToDBNull(item.File_Type)
            cmd.Parameters(4).Value = nothingToDBNull(item.SizeMB)
            cmd.Parameters(5).Value = nothingToDBNull(item.Media_Path_ID)
            cmd.Parameters(6).Value = nothingToDBNull(item.Merge_GUID)
            cmd.Parameters(7).Value = nothingToDBNull(item.Parent_GUID)
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.ExecuteNonQuery()
        Next
        If isObs Then
            cmd.CommandText = "INSERT INTO MLO_Media (MLO_ID, Media_ID) SELECT MLO_ID, Media_ID FROM Media INNER JOIN MLO ON Media.Parent_GUID=MLO.Merge_GUID WHERE Media_ID NOT IN (SELECT Media_ID FROM MLO_Media);"
        Else
            cmd.CommandText = "INSERT INTO MLI_Media (MLI_ID, Media_ID) SELECT MLI_ID, Media_ID FROM Media INNER JOIN MLI ON Media.Parent_GUID=MLI.Merge_GUID WHERE Media_ID NOT IN (SELECT Media_ID FROM MLI_Media);"
        End If

        'cmd.CommandText = "UPDATE MLO INNER JOIN MLI ON MLO.Parent_GUID=MLI.Merge_GUID SET MLO.MLI_ID=MLI.MLI_ID;"
        cmd.ExecuteNonQuery()
    End Sub

    Private Function parametersM(ByVal cmd As OleDb.OleDbCommand) As OleDb.OleDbParameter()

        Dim par(7) As OleDb.OleDbParameter
        For i = 0 To 7
            par(i) = cmd.CreateParameter
            par(i).ParameterName = String.Format("@{0}", i)
        Next i
        par(0).DbType = DbType.String
        par(1).DbType = DbType.String
        par(2).DbType = DbType.String
        par(3).DbType = DbType.String
        par(4).DbType = DbType.String
        par(5).DbType = DbType.Int32
        par(6).DbType = DbType.String
        par(7).DbType = DbType.String
        Return par
    End Function

    Private Sub writeO(ByVal cmd As OleDb.OleDbCommand, ByVal obs As List(Of ITobs))
        cmd.Parameters.AddRange(parametersO(cmd))
        UpdateProjectTable(cmd.Connection, "MLO")
        cmd.CommandText = "INSERT INTO MLO (Clock_From, Clock_To, Value_1st_Dimension, Value_2nd_Dimension, Value_Percent, Code, Distance, Remarks, Continuous, Joint, IsReverse, " _
            & "Grade, Observation_Text, Digital_Time, Merge_GUID, Parent_GUID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        For Each item In obs
            cmd.Parameters(0).Value = nothingToDBNull(item.Clock_From)
            cmd.Parameters(1).Value = nothingToDBNull(item.Clock_To)
            cmd.Parameters(2).Value = nothingToDBNull(item.Value_1st_Dimension)
            cmd.Parameters(3).Value = nothingToDBNull(item.Value_2nd_Dimension)
            cmd.Parameters(4).Value = nothingToDBNull(item.Value_Percent)
            cmd.Parameters(5).Value = Left(item.Code, 20)
            cmd.Parameters(6).Value = nothingToDBNull(item.Distance)
            cmd.Parameters(7).Value = nothingToDBNull(item.Remarks)
            cmd.Parameters(8).Value = nothingToDBNull(item.Continuous)
            cmd.Parameters(9).Value = nothingToDBNull(item.Joint)
            cmd.Parameters(10).Value = nothingToDBNull(item.IsReverse)
            cmd.Parameters(11).Value = nothingToDBNull(item.Grade)
            cmd.Parameters(12).Value = nothingToDBNull(item.Observation_Text)
            cmd.Parameters(13).Value = nothingToDBNull(item.Digital_Time)
            cmd.Parameters(14).Value = nothingToDBNull(item.Merge_GUID)
            cmd.Parameters(15).Value = nothingToDBNull(item.Parent_GUID)
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.ExecuteNonQuery()
        Next
        cmd.CommandText = "UPDATE MLO INNER JOIN MLI ON MLO.Parent_GUID=MLI.Merge_GUID SET MLO.MLI_ID=MLI.MLI_ID;"
        cmd.ExecuteNonQuery()
    End Sub

    Private Sub writeI(ByVal cmd As OleDb.OleDbCommand, ByVal insp As List(Of ITinspections))
        cmd.Parameters.AddRange(parametersI(cmd))
        UpdateProjectTable(cmd.Connection, "MLI")
        'cmd.CommandText = "INSERT INTO MLI (Sheet_Number, Certificate_Number, Customer, Cleaned, Inspection_Date, Start_Time, Last_Modified_Date, Clean_Date, Creation_Date, " _
        '    & "Inspection_Direction, Media_Number, Flow_Control, Operator, PACP_Custom_1, PACP_Custom_2, PACP_Custom_3, PACP_Custom_4, PACP_Custom_5, Reason_of_Inspection, " _
        '    & "Reverse_Setup, Weather, WO_Number, PO_Number, Inspected_Length, Additional_Info, Merge_GUID, Parent_GUID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        cmd.CommandText = "INSERT INTO MLI (Sheet_Number, Certificate_Number, Customer, Cleaned, Inspection_Date, Start_Time, Last_Modified_Date, Clean_Date, Creation_Date, " _
            & "Inspection_Direction, Media_Number, Flow_Control, Operator, Reason_of_Inspection, " _
            & "Reverse_Setup, Weather, WO_Number, PO_Number, Inspected_Length, Additional_Info, Merge_GUID, Parent_GUID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        For Each item In insp
            cmd.Parameters(0).Value = nothingToDBNull(item.Sheet_Number)
            cmd.Parameters(1).Value = nothingToDBNull(item.Certificate_Number)
            cmd.Parameters(2).Value = nothingToDBNull(item.Customer)
            cmd.Parameters(3).Value = nothingToDBNull(item.Cleaned)
            cmd.Parameters(4).Value = nothingToDBNull(item.Inspection_Date)
            cmd.Parameters(5).Value = nothingToDBNull(item.Start_Time)
            cmd.Parameters(6).Value = nothingToDBNull(item.Last_Modified_Date)
            cmd.Parameters(7).Value = nothingToDBNull(item.Clean_Date)
            cmd.Parameters(8).Value = nothingToDBNull(item.Creation_Date)
            cmd.Parameters(9).Value = nothingToDBNull(item.Inspection_Direction)
            cmd.Parameters(10).Value = nothingToDBNull(item.Media_Number)
            cmd.Parameters(11).Value = nothingToDBNull(item.Flow_Control)
            cmd.Parameters(12).Value = nothingToDBNull(item.TVOperator)
            cmd.Parameters(13).Value = nothingToDBNull(item.PACP_Custom_1)
            cmd.Parameters(14).Value = nothingToDBNull(item.PACP_Custom_2)
            'cmd.Parameters(15).Value = nothingToDBNull(item.PACP_Custom_3)
            'cmd.Parameters(16).Value = nothingToDBNull(item.PACP_Custom_4)
            'cmd.Parameters(17).Value = nothingToDBNull(item.PACP_Custom_5)
            cmd.Parameters(13).Value = nothingToDBNull(item.Reason_of_Inspection)
            cmd.Parameters(14).Value = nothingToDBNull(item.Reverse_Setup)
            cmd.Parameters(15).Value = nothingToDBNull(item.Weather)
            cmd.Parameters(16).Value = nothingToDBNull(item.WO_Number)
            cmd.Parameters(17).Value = nothingToDBNull(item.PO_Number)
            cmd.Parameters(18).Value = nothingToDBNull(item.Inspected_Length)
            cmd.Parameters(19).Value = nothingToDBNull(item.Additional_Info)
            cmd.Parameters(20).Value = nothingToDBNull(item.Merge_GUID)
            cmd.Parameters(21).Value = nothingToDBNull(item.Parent_GUID)
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.ExecuteNonQuery()
        Next
        cmd.CommandText = "UPDATE MLI INNER JOIN ML ON MLI.Parent_GUID=ML.Merge_GUID SET MLI.ML_ID=ML.ML_ID;"
        cmd.ExecuteNonQuery()
    End Sub

    Private Sub writeA(ByVal cmd As OleDb.OleDbCommand, ByVal asset As List(Of ITassets))
        cmd.Parameters.AddRange(parametersA(cmd))
        UpdateProjectTable(cmd.Connection, "ML")
        cmd.CommandText = "INSERT INTO ML (Asset_Use, City, Drainage_Area, DS_MH, DS_GradetoInvert, DS_RimtoGrade, DS_RimtoInvert, Joint_Length, Lining, Location, " _
            & "Location_Details, Map_1, Material, ML_Name, Owner,Pipe_Height, Pipe_Shape, Pipe_Width, Section_Length, Sewer_Category, Street, US_MH, US_RimtoInvert, " _
            & "US_GradetoInvert, US_RimtoGrade, Year_Constructed, Year_Renewed, Merge_GUID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        For Each item In asset
            cmd.Parameters(0).Value = nothingToDBNull(item.Asset_Use)
            cmd.Parameters(1).Value = nothingToDBNull(item.City)
            cmd.Parameters(2).Value = nothingToDBNull(item.Drainage_Area)
            cmd.Parameters(3).Value = nothingToDBNull(item.DS_MH)
            cmd.Parameters(4).Value = nothingToDBNull(item.DS_GradetoInvert)
            cmd.Parameters(5).Value = nothingToDBNull(item.DS_RimtoGrade)
            cmd.Parameters(6).Value = nothingToDBNull(item.DS_RimtoInvert)
            cmd.Parameters(7).Value = nothingToDBNull(item.Joint_Length)
            cmd.Parameters(8).Value = nothingToDBNull(item.Lining)
            cmd.Parameters(9).Value = nothingToDBNull(item.Location)
            cmd.Parameters(10).Value = nothingToDBNull(item.Location_Details)
            cmd.Parameters(11).Value = nothingToDBNull(item.Map_1)
            cmd.Parameters(12).Value = nothingToDBNull(item.Material)
            cmd.Parameters(13).Value = nothingToDBNull(item.ML_Name)
            cmd.Parameters(14).Value = nothingToDBNull(item.Owner)
            cmd.Parameters(15).Value = nothingToDBNull(item.Pipe_Height)
            cmd.Parameters(16).Value = nothingToDBNull(item.Pipe_Shape)
            cmd.Parameters(17).Value = nothingToDBNull(item.Pipe_Width)
            cmd.Parameters(18).Value = nothingToDBNull(item.Section_Length)
            cmd.Parameters(19).Value = nothingToDBNull(item.Sewer_Category)
            cmd.Parameters(20).Value = nothingToDBNull(item.Street)
            cmd.Parameters(21).Value = nothingToDBNull(item.US_MH)
            cmd.Parameters(22).Value = nothingToDBNull(item.US_RimtoInvert)
            cmd.Parameters(23).Value = nothingToDBNull(item.US_GradetoInvert)
            cmd.Parameters(24).Value = nothingToDBNull(item.US_RimtoGrade)
            cmd.Parameters(25).Value = nothingToDBNull(item.Year_Constructed)
            cmd.Parameters(26).Value = nothingToDBNull(item.Year_Renewed)
            cmd.Parameters(27).Value = nothingToDBNull(item.Merge_GUID)
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.ExecuteNonQuery()
        Next
    End Sub

    Private Function nothingToDBNull(ByVal obj As Object) As Object
        If obj Is Nothing Then
            Return DBNull.Value
        Else
            Return obj
        End If
    End Function

    Private Function parametersO(ByVal cmd As OleDb.OleDbCommand) As OleDb.OleDbParameter()

        Dim par(15) As OleDb.OleDbParameter
        For i = 0 To 15
            par(i) = cmd.CreateParameter
            par(i).ParameterName = String.Format("@{0}", i)
        Next i
        par(0).DbType = DbType.Int32
        par(1).DbType = DbType.Int32
        par(2).DbType = DbType.Int32
        par(3).DbType = DbType.Int32
        par(4).DbType = DbType.Int32
        par(5).DbType = DbType.String
        par(6).DbType = DbType.Single
        par(7).DbType = DbType.String
        par(8).DbType = DbType.String
        par(9).DbType = DbType.Boolean
        par(10).DbType = DbType.Boolean
        par(11).DbType = DbType.Int32
        par(12).DbType = DbType.String
        par(13).DbType = DbType.String
        par(14).DbType = DbType.String
        par(15).DbType = DbType.String
        Return par
    End Function

    Private Function parametersI(ByVal cmd As OleDb.OleDbCommand) As OleDb.OleDbParameter()

        Dim par(26) As OleDb.OleDbParameter
        For i = 0 To 26
            par(i) = cmd.CreateParameter
            par(i).ParameterName = String.Format("@{0}", i)
        Next i
        par(0).DbType = DbType.Int32 'sheet number
        par(1).DbType = DbType.String 'cert no
        par(2).DbType = DbType.String 'customer
        par(3).DbType = DbType.String 'cleaned
        par(4).DbType = DbType.DateTime 'inspection date
        par(5).DbType = DbType.DateTime 'start time
        par(6).DbType = DbType.DateTime 'last modified
        par(7).DbType = DbType.DateTime 'clean
        par(8).DbType = DbType.DateTime 'creation
        par(9).DbType = DbType.String 'direction
        par(10).DbType = DbType.String 'media no
        par(11).DbType = DbType.String 'flow control
        par(12).DbType = DbType.String 'operator
        par(13).DbType = DbType.String 'cus 1
        par(14).DbType = DbType.String 'cus 2
        'par(15).DbType = DbType.String 'cus 3
        'par(16).DbType = DbType.String 'cus 4
        'par(17).DbType = DbType.String 'cus 5
        par(13).DbType = DbType.String 'reason
        par(14).DbType = DbType.Int32 'reverse
        par(15).DbType = DbType.String 'weather
        par(16).DbType = DbType.String 'wono
        par(17).DbType = DbType.String 'pono
        par(18).DbType = DbType.Single 'inspected length
        par(19).DbType = DbType.String 'addinfo
        par(20).DbType = DbType.String 'merge_guid
        par(21).DbType = DbType.String 'Parent_guid
        Return par
    End Function
    Private Function parametersA(ByVal cmd As OleDb.OleDbCommand) As OleDb.OleDbParameter()

        Dim par(27) As OleDb.OleDbParameter
        For i = 0 To 27
            par(i) = cmd.CreateParameter
            par(i).ParameterName = String.Format("@{0}", i)
        Next i
        par(0).DbType = DbType.String
        par(1).DbType = DbType.String
        par(2).DbType = DbType.String
        par(3).DbType = DbType.String
        par(4).DbType = DbType.Double
        par(5).DbType = DbType.Double
        par(6).DbType = DbType.Double
        par(7).DbType = DbType.Double
        par(8).DbType = DbType.String
        par(9).DbType = DbType.String
        par(10).DbType = DbType.String
        par(11).DbType = DbType.String
        par(12).DbType = DbType.String
        par(13).DbType = DbType.String
        par(14).DbType = DbType.String
        par(15).DbType = DbType.Int32
        par(16).DbType = DbType.String
        par(17).DbType = DbType.Int32
        par(18).DbType = DbType.Double
        par(19).DbType = DbType.String
        par(20).DbType = DbType.String
        par(21).DbType = DbType.String
        par(22).DbType = DbType.Single
        par(23).DbType = DbType.Single
        par(24).DbType = DbType.Single
        par(25).DbType = DbType.Int32
        par(26).DbType = DbType.Int32
        par(27).DbType = DbType.String
        Return par
    End Function
    Private Function getVersion(ByVal con As Common.DbConnection) As Integer
        Dim cmd As Common.DbCommand = con.CreateCommand
        cmd.CommandText = "SELECT VERSION FROM DB_VERSION_STAMP;"
        If con.State <> ConnectionState.Open Then
            con.Open()
        End If
        Return CInt(cmd.ExecuteScalar)
        con.Close()
    End Function
    Private Function MLConvert17(ByVal dt As DataTable) As List(Of ITassets)

        Dim ql = From a As DataRow In dt.AsEnumerable.AsParallel
                 Select New ITassets With {.Merge_GUID = Guid.NewGuid.ToString, .ML_ID = nullInteger(a.Item("__Key")), .Pipe_Shape = codeLookup(a.Item("PipeShape")), .Asset_Use = codeLookup(a.Item("UseOfSewer")), .City = a.Item("City").ToString, .Drainage_Area = a.Item("DrainageArea").ToString, .DS_MH = holeLookup(a.Item("DownstreamManhole")), .Joint_Length = nullSingle(a.Item("JointDistance")), .Location = codeLookup(a.Item("Loc")), .Lining = codeLookup(a.Item("LnMethod")), .Location_Details = codeLookup(a.Item("SurfaceType")), .Material = codeLookup(a.Item("PipeType")), .ML_Name = a.Item("SegmentID").ToString, .Owner = a.Item("Owner").ToString, .Pipe_Height = nullInteger(a.Item("Height")), .Pipe_Width = nullInteger(a.Item("Width")), .Section_Length = nullSingle(a.Item("AssetLength")), .Sewer_Category = codeLookup(a.Item("SewerCategory")), .Street = a.Item("Address").ToString, .US_MH = holeLookup(a.Item("UpstreamManhole")), .Year_Constructed = nullDate(a.Item("ConstructionYear")).Year, .Year_Renewed = getDate(a.Item("YearRehabilitated"))}

        Return ql.ToList

    End Function
    Private Function MLIConvert17(ByVal dt As DataTable, ByVal assets As List(Of ITassets)) As List(Of ITinspections)
        Dim ql = From i As DataRow In dt.AsEnumerable.AsParallel
                 Join a In assets.AsEnumerable.AsParallel
                 On CInt(i.Item("Asset")) Equals a.ML_ID
                 Select New ITinspections With {.VideoMedia = nullInteger(i.Item("FirstVideoMedia")), .Merge_GUID = Guid.NewGuid.ToString, .ML_ID = nullInteger(i.Item("Asset")), .MLI_ID = nullInteger(i.Item("__Key")), .Inspection_Date = nullDate(i.Item("Date")), .Creation_Date = nullDate(i.Item("Date")), .Additional_Info = i.Item("Comment").ToString, .Certificate_Number = i.Item("SurCertNo").ToString, .Clean_Date = nullDate(i.Item("DateCleaned")), .Cleaned = codeLookup(i.Item("PreCleaning")), .Flow_Control = codeLookup(i.Item("FlowControl")), .Inspected_Length = nullSingle(i.Item("SurveyedFootage")), .Media_Number = i.Item("MediaLabel").ToString, .tvOperator = codeLookup(i.Item("Operator")), .WO_Number = i.Item("WorkOrder").ToString, .Reason_of_Inspection = codeLookup(i.Item("Reason")), .Start_Time = nullDate(i.Item("DateStart")), .Weather = codeLookup(i.Item("Weather")), .Sheet_Number = nullInteger(i.Item("Sheet_Number")), .Parent_GUID = a.Merge_GUID}

        Return ql.ToList

    End Function

    Private Function MLOConvert17(ByVal dt As DataTable, ByVal ins As List(Of ITinspections)) As List(Of ITobs)
        Dim ql = From o As DataRow In dt.AsEnumerable.AsParallel
                 Join i In ins.AsEnumerable.AsParallel
                 On CInt(o.Item("Inspection")) Equals i.MLI_ID
                 Select New ITobs With {.Merge_GUID = Guid.NewGuid.ToString, .MLI_ID = nullInteger(o.Item("Inspection")), .MLO_ID = nullInteger(o.Item("__Key")), .Distance = nullSingle(o.Item("Distance")), .Remarks = o.Item("Comment").ToString, .Value_1st_Dimension = nullInteger(o.Item("Dimension1")), .Value_2nd_Dimension = nullInteger(o.Item("Dimension2")), .Value_Percent = nullInteger(o.Item("Percentage")), .Joint = CBool(o.Item("Joint")), .Clock_From = nullInteger(o.Item("ClockFrom")), .Clock_To = nullInteger(o.Item("ClockTo")), .Code = codeLookup(o.Item("Code"), False), .Observation_Text = codeLookup(o.Item("Code"), False), .IsReverse = CBool(o.Item("Reversed")), .Digital_Time = "", .Parent_GUID = i.Merge_GUID}

        Return ql.ToList
    End Function

    Private Function nullSingle(ByVal obj As Object) As Single
        If IsDBNull(obj) Then
            Return Nothing
        Else
            Return CSng(obj)
        End If
    End Function

    Private Function nullDate(ByVal obj As Object) As DateTime
        If IsDBNull(obj) Then
            Return Nothing
        Else
            Return CDate(obj)
        End If
    End Function

    Private Function nullInteger(ByVal obj As Object) As Integer
        If IsDBNull(obj) Then
            Return Nothing
        Else
            Return CInt(obj)
        End If
    End Function

    Private Function MediaConvert17(ByVal dt As DataTable, ByVal ins As List(Of ITinspections)) As List(Of ITmedia)

        Dim ql = From m As DataRow In dt.AsEnumerable.AsParallel
        Join i In ins.AsEnumerable.AsParallel
        On CInt(m.Item("__Key")) Equals i.VideoMedia
                 Select New ITmedia With {.Parent_GUID = i.Merge_GUID, .Merge_GUID = Guid.NewGuid.ToString, .File_Name = m.Item("FullPath").ToString, .File_Path = "\Media\Video\", .Media_ID = nullInteger(m.Item("__Key")), .File_Type = "Video", .Media_Path_ID = 1}

        Return ql.ToList

    End Function

    Private Function getDate(ByVal obj As Object) As Integer
        Dim d As DateTime
        Try
            d = CDate(obj)
            Return d.Year
        Catch ex As Exception
            Return 1900
        End Try
    End Function
    Private Function codeLookup(ByVal intKey As Object, Optional ByVal desc As Boolean = True) As String

        Try
            If IsDBNull(intKey) Then
                Return ""
            End If
            Dim int As Integer = CInt(intKey)
            If int = 0 Or int = -1 Then
                Return ""
            End If

            Dim qlCode = (From codes In dtCodes.AsEnumerable()
                          Where CInt(codes("KeyID")) = int
                          Select New With {.code = CStr(codes("Code")), .desc = codes("Description").ToString})

            If desc = False OrElse qlCode.First.desc.Length = 0 Then
                Return qlCode.First.code
            Else
                Return qlCode.First.desc
            End If
        Catch
            Return String.Empty
        End Try

    End Function
    Private Function holeLookup(ByVal intKey As Object) As String
        If IsDBNull(intKey) Then
            Return ""
        End If
        Dim int As Integer = CInt(intKey)
        If int = 0 Or int = -1 Then
            Return ""
        End If

        Dim qlCode = (From codes In dtHoles.AsEnumerable()
                      Where CInt(codes("KeyID")) = int
                      Select CStr(codes("ManholeID")))

        Return qlCode.First

    End Function

    Public Shared Sub UpdateProjectTable(ByRef con As OleDb.OleDbConnection, t As String)
        'add to datatable.
        'If updateDT.Columns.Contains("Merge_GUID") = False Then
        'updateDT.Columns.Add("Merge_GUID", Type.GetType("System.String"))

        'if addToDB is true, add item to database

        Dim cmd As OleDb.OleDbCommand = con.CreateCommand()
        Try
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.CommandText = String.Format("ALTER TABLE {0} ADD Merge_GUID varchar(36);", t)
            cmd.ExecuteNonQuery()
        Catch ex As Exception

        End Try

        Try
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.CommandText = String.Format("ALTER TABLE {0} ADD Parent_GUID varchar(36);", t)
            cmd.ExecuteNonQuery()
        Catch ex As Exception

        End Try

    End Sub

    Public Shared Sub DowndateProjectTable(ByRef con As OleDb.OleDbConnection, t As String)
        'add to datatable.
        'If updateDT.Columns.Contains("Merge_GUID") = False Then
        'updateDT.Columns.Add("Merge_GUID", Type.GetType("System.String"))

        'if addToDB is true, add item to database

        Dim cmd As OleDb.OleDbCommand = con.CreateCommand()
        'Try
        '    If cmd.Connection.State <> ConnectionState.Open Then
        '        cmd.Connection.Open()
        '    End If
        '    cmd.CommandText = String.Format("ALTER TABLE {0} DROP COLUMN Merge_GUID;", t)
        '    cmd.ExecuteNonQuery()
        'Catch ex As Exception

        'End Try

        Try
            If cmd.Connection.State <> ConnectionState.Open Then
                cmd.Connection.Open()
            End If
            cmd.CommandText = String.Format("ALTER TABLE {0} DROP COLUMN Parent_GUID;", t)
            cmd.ExecuteNonQuery()
        Catch ex As Exception

        End Try

    End Sub

    Private Function MLConvert35(ByVal dt As DataTable) As List(Of ITassets)

        'Dim ql = From a As DataRow In dt.AsEnumerable() Select New ITassets With {.Merge_GUID = Guid.NewGuid.ToString, .ML_ID = nullInteger(a.Item("KEY")), .Pipe_Shape = codeLookup(a.Item("PIPE_SHAPE")), .Asset_Use = codeLookup(a.Item("USE_OF_SEWER")), .City = a.Item("CITY").ToString, .Drainage_Area = a.Item("DRAINAGE_AREA").ToString, .DS_MH = holeLookup(a.Item("DOWNSTREAM_MANHOLE")), .Joint_Length = nullSingle(a.Item("JOINT_DISTANCE")), .Location = codeLookup(a.Item("LOC")), .Lining = codeLookup(a.Item("LN_METHOD")), .Location_Details = codeLookup(a.Item("SURFACE_TYPE")), .Material = codeLookup(a.Item("PIPE_TYPE")), .ML_Name = a.Item("SEGMENTID").ToString, .Owner = a.Item("OWNER").ToString, .Pipe_Height = nullInteger(a.Item("HEIGHT")), .Pipe_Width = nullInteger(a.Item("WIDTH")), .Section_Length = nullSingle(a.Item("ASSET_LENGTH")), .Sewer_Category = codeLookup(a.Item("SEWER_CATEGORY")), .Street = a.Item("ADDRESS").ToString, .US_MH = holeLookup(a.Item("UPSTREAM_MANHOLE")), .Year_Constructed = nullDate(a.Item("CONSTRUCTION_YEAR")).Year, .Year_Renewed = getDate(a.Item("YEAR_REHABILITATED"))}
        Dim ql As New List(Of ITassets)
        Dim Asset As ITassets
        For Each row As DataRow In dt.Rows
            Asset = New ITassets()

            Asset.Merge_GUID = Guid.NewGuid.ToString()
            Asset.ML_ID = nullInteger(row("KEY"))
            Asset.Pipe_Shape = codeLookup(row("PIPE_SHAPE"))
            Asset.Asset_Use = codeLookup(row("USE_OF_SEWER"))
            Asset.City = row("CITY").ToString
            Asset.Drainage_Area = row("DRAINAGE_AREA").ToString
            Asset.DS_MH = holeLookup(row("DOWNSTREAM_MANHOLE"))
            Asset.Joint_Length = nullSingle(row("JOINT_DISTANCE"))
            Asset.Location = codeLookup(row("LOC"))
            Asset.Lining = codeLookup(row("LN_METHOD"))
            Asset.Location_Details = codeLookup(row("SURFACE_TYPE"))
            Asset.Material = codeLookup(row("PIPE_TYPE"))
            Asset.ML_Name = row("SEGMENTID").ToString
            Asset.Owner = row("OWNER").ToString
            Asset.Pipe_Height = nullInteger(row("HEIGHT"))
            Asset.Pipe_Width = nullInteger(row("WIDTH"))
            Asset.Section_Length = nullSingle(row("ASSET_LENGTH"))
            Asset.Sewer_Category = codeLookup(row("SEWER_CATEGORY"))
            Asset.Street = row("ADDRESS").ToString
            Asset.US_MH = holeLookup(row("UPSTREAM_MANHOLE"))
            Asset.Year_Constructed = nullDate(row("CONSTRUCTION_YEAR")).Year
            Asset.Year_Renewed = getDate(row("YEAR_REHABILITATED"))

            ql.Add(Asset)

        Next row

        Return ql.ToList

    End Function
    Private Function MLIConvert35(ByVal dt As DataTable, ByVal assets As List(Of ITassets)) As List(Of ITinspections)
        'Dim ql = From i As DataRow In dt.AsEnumerable.AsParallel
        '         Join a In assets.AsEnumerable.AsParallel
        '         On CInt(i.Item("Asset")) Equals a.ML_ID
        '         Select New ITinspections With {
        '.VideoMedia = nullInteger(i.Item("FIRST_VIDEO_MEDIA")), .Merge_GUID = Guid.NewGuid.ToString, .ML_ID = nullInteger(i.Item("ASSET")), .MLI_ID = nullInteger(i.Item("KEY")), .Inspection_Date = nullDate(i.Item("DATE_START")), .Creation_Date = nullDate(i.Item("DATE_START")), .Additional_Info = i.Item("COMMENT").ToString, .Certificate_Number = i.Item("SUR_CERT_NO").ToString, .Clean_Date = nullDate(i.Item("DATE_CLEANED")), .Cleaned = codeLookup(i.Item("PRE_CLEANING")), .Flow_Control = codeLookup(i.Item("FLOW_CONTROL")), .Inspected_Length = nullSingle(i.Item("SURVEYED_FOOTAGE")), .Media_Number = i.Item("MEDIA_LABEL").ToString, .TVOperator = codeLookup(i.Item("OPERATOR")), .WO_Number = i.Item("WORK_ORDER").ToString, .Reason_of_Inspection = codeLookup(i.Item("REASON")), .Start_Time = nullDate(i.Item("DATE_START")), .Weather = codeLookup(i.Item("WEATHER")), .Sheet_Number = nullInteger(i.Item("SHEET_NUMBER")), .Parent_GUID = a.Merge_GUID}
        Dim theAsset As ITassets
        Dim ql As New List(Of ITinspections)
        Dim Inspections As ITinspections
        For Each row As DataRow In dt.Rows
            Inspections = New ITinspections()

            Inspections.VideoMedia = nullInteger(row("FIRST_VIDEO_MEDIA"))
            Inspections.Merge_GUID = Guid.NewGuid.ToString
            Inspections.ML_ID = nullInteger(row("ASSET"))
            Inspections.MLI_ID = nullInteger(row("KEY"))
            Inspections.Inspection_Date = nullDate(row("DATE_START"))
            Inspections.Creation_Date = nullDate(row("DATE_START"))
            Inspections.Additional_Info = row("COMMENT").ToString
            Inspections.Certificate_Number = row("SUR_CERT_NO").ToString
            Inspections.Clean_Date = nullDate(row("DATE_CLEANED"))
            Inspections.Cleaned = codeLookup(row("PRE_CLEANING"))
            Inspections.Flow_Control = codeLookup(row("FLOW_CONTROL"))
            Inspections.Inspected_Length = nullSingle(row("SURVEYED_FOOTAGE"))
            Inspections.Media_Number = row("MEDIA_LABEL").ToString
            Inspections.TVOperator = codeLookup(row("OPERATOR"))
            Inspections.WO_Number = row("WORK_ORDER").ToString
            Inspections.Reason_of_Inspection = codeLookup(row("REASON"))
            Inspections.Start_Time = nullDate(row("DATE_START"))
            Inspections.Weather = codeLookup(row("WEATHER"))
            Inspections.Sheet_Number = nullInteger(row("SHEET_NUMBER"))
            Inspections.PACP_Custom_1 = nullInteger(row("Insp_No"))
            Inspections.PACP_Custom_2 = nullInteger(row("Crew"))


            theAsset = assets.Find(Function(p) p.ML_ID = Inspections.ML_ID)
            Inspections.Parent_GUID = theAsset.Merge_GUID

            ql.Add(Inspections)

        Next row

        Return ql.ToList

    End Function

    Private Function MLOConvert35(ByVal dt As DataTable, ByVal ins As List(Of ITinspections)) As List(Of ITobs)
        'Dim ql = From o As DataRow In dt.AsEnumerable.AsParallel
        '         Join i In ins.AsEnumerable.AsParallel
        '         On CInt(o.Item("Inspection")) Equals i.MLI_ID
        '         Select New ITobs With {.Merge_GUID = Guid.NewGuid.ToString, .MLI_ID = nullInteger(o.Item("INSPECTION")), .MLO_ID = nullInteger(o.Item("KEY")), .Distance = nullSingle(o.Item("DISTANCE")), .Remarks = o.Item("COMMENT").ToString, .Value_1st_Dimension = nullInteger(o.Item("DIMENSION1")), .Value_2nd_Dimension = nullInteger(o.Item("DIMENSION2")), .Value_Percent = nullInteger(o.Item("PERCENTAGE")), .Joint = CBool(o.Item("JOINT")), .Clock_From = nullInteger(o.Item("CLOCK_FROM")), .Clock_To = nullInteger(o.Item("CLOCK_TO")), .Code = codeLookup(o.Item("CODE"), False), .Observation_Text = codeLookup(o.Item("CODE"), False), .IsReverse = CBool(o.Item("REVERSED")), .Digital_Time = "", .Parent_GUID = i.Merge_GUID}

        Dim theInspection As ITinspections
        Dim ql As New List(Of ITobs)
        Dim Observations As ITobs
        For Each row As DataRow In dt.Rows
            Observations = New ITobs()

            Observations.Merge_GUID = Guid.NewGuid.ToString
            Observations.MLI_ID = nullInteger(row("INSPECTION"))
            Observations.MLO_ID = nullInteger(row("KEY"))
            Observations.Distance = nullSingle(row("DISTANCE"))
            Observations.Remarks = row("COMMENT").ToString
            Observations.Value_1st_Dimension = nullInteger(row("DIMENSION1"))
            Observations.Value_2nd_Dimension = nullInteger(row("DIMENSION2"))
            Observations.Value_Percent = nullInteger(row("PERCENTAGE"))
            Observations.Joint = CBool(row("JOINT"))
            Observations.Clock_From = nullInteger(row("CLOCK_FROM"))
            Observations.Clock_To = nullInteger(row("CLOCK_TO"))
            Observations.Code = codeLookup(row("CODE"), False)
            Observations.Observation_Text = codeLookup(row("CODE"), False)
            Observations.IsReverse = CBool(row("REVERSED"))
            Observations.Digital_Time = String.Empty


            theInspection = ins.Find(Function(p) p.MLI_ID = Observations.MLI_ID)
            Observations.Parent_GUID = theInspection.Merge_GUID

            ql.Add(Observations)

        Next row


        Return ql.ToList
    End Function

    Private Function MediaConvert35(ByVal dt As DataTable, ByVal ins As List(Of ITinspections)) As List(Of ITmedia)
        'Dim ql = From m As DataRow In dt.AsEnumerable.AsParallel
        '         Join i In ins.AsEnumerable.AsParallel
        '         On CInt(m.Item("Key")) Equals i.VideoMedia
        '         Select New ITmedia With {
        '.Parent_GUID = i.Merge_GUID,
        '.Merge_GUID = Guid.NewGuid.ToString,
        '.File_Name = m.Item("FULL_PATH").ToString,
        '.File_Path = "\Media\Video\",
        '.Media_ID = nullInteger(m.Item("Key")),
        '.File_Type = "Video",
        '.Media_Path_ID = 1}

        Dim theInspection As ITinspections
        Dim ql As New List(Of ITmedia)
        Dim Media As ITmedia
        For Each row As DataRow In dt.Rows
            Media = New ITmedia()

            Media.Merge_GUID = Guid.NewGuid.ToString
            Media.File_Name = row("FULL_PATH").ToString
            Media.File_Path = "\Media\Video\"
            Media.Media_ID = nullInteger(row("Key"))
            Media.File_Type = "Video"
            Media.Media_Path_ID = 1

            theInspection = ins.Find(Function(p) p.VideoMedia = Media.Media_ID)
            Media.Parent_GUID = theInspection.Merge_GUID

            ql.Add(Media)

        Next row


        Return ql.ToList

    End Function

    Private Function obsMediaConvert35(ByVal dt As DataTable, ByVal ins As List(Of ITobs)) As List(Of ITmedia)

        'Dim ql = From m As DataRow In dt.AsEnumerable.AsParallel
        'Join i In ins.AsEnumerable.AsParallel
        'On CInt(m.Item("CONTAINERKEY")) Equals i.MLO_ID
        '         Select New ITmedia With {.Parent_GUID = i.Merge_GUID, .Merge_GUID = Guid.NewGuid.ToString, .File_Name = m.Item("FULL_PATH").ToString, .File_Path = "\Media\Photos\", .Media_ID = nullInteger(m.Item("KeyID")), .File_Type = "SnapShot", .Media_Path_ID = 1}
        Dim theObservetion As ITobs
        Dim ql As New List(Of ITmedia)
        Dim Media As ITmedia
        For Each row As DataRow In dt.Rows
            Media = New ITmedia()

            Media.Merge_GUID = Guid.NewGuid.ToString
            Media.File_Name = row("FULL_PATH").ToString
            Media.File_Path = "\Media\Photos\"
            Media.Media_ID = nullInteger(row("KeyID"))
            Media.File_Type = "SnapShot"
            Media.Media_Path_ID = 1

            theObservetion = ins.Find(Function(p) p.MLO_ID = CInt(row("CONTAINERKEY")))
            Media.Parent_GUID = theObservetion.Merge_GUID

            ql.Add(Media)

        Next row

        Return ql.ToList

    End Function

End Class