using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Data;
using System.Data.OleDb;
using System.ComponentModel;
using System.IO;

namespace Granite_batch_convert_Csharp_VN
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public BackgroundWorker backWorker;
        private string resourceITPDatabase = string.Empty;

        public MainWindow()
        {
            InitializeComponent();
        }

        private void butSelectFolder_Click(object sender, RoutedEventArgs e)
        {
            var selectFolder = new System.Windows.Forms.FolderBrowserDialog();
            System.Windows.Forms.DialogResult result = selectFolder.ShowDialog();
            if (result.Equals(System.Windows.Forms.DialogResult.OK))
            {
                string folderPath = selectFolder.SelectedPath;
                if (folderPath[folderPath.Length - 1] != '\\') { folderPath += "\\"; }
                tboxSelectedFolder.Text = folderPath;
            }
        }

        private void butEngage_Click(object sender, RoutedEventArgs e)
        {
            string[] allFoundMDBFiles = System.IO.Directory.GetFiles(tboxSelectedFolder.Text, "*.mdb", System.IO.SearchOption.AllDirectories);
            Parallel.ForEach(allFoundMDBFiles.AsEnumerable(), new ParallelOptions { MaxDegreeOfParallelism = 4 }, curDB =>
                {
                    try
                    {
                        convertDb(curDB);
                    }
                    catch  (Exception ex)
                    {
                        System.Console.WriteLine(ex);
                    }
                });
        }

        private string _getItDbPath()
        {
            var dlg = new Microsoft.Win32.OpenFileDialog();
            dlg.Filter = "ITpipes Db|*.mdb";
            dlg.Title = "Select the target ITpipes Database";
            var result = dlg.ShowDialog();

            if (result == true)
            {
                return dlg.FileName;
            }

            return null;
        }

        private void convertDb(string sourceDatabase)
        {

            vb_dll_library_project_the_real_one.GraniteConvert granConverter = new vb_dll_library_project_the_real_one.GraniteConvert();
            OleDbConnection sourceConn = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;User ID=Admin;Jet OLEDB:Database Password=dbadmin;Data Source=" + sourceDatabase);

        
            if (string.IsNullOrEmpty(resourceITPDatabase))
                resourceITPDatabase  = _getItDbPath();

            if (!File.Exists(resourceITPDatabase))
            {
                System.Windows.Forms.MessageBox.Show("Can't convert without an ITpipes database to use as a target");
            }
            string targetDatabase = resourceITPDatabase;
            //if (File.Exists(targetDatabase)) { System.IO.File.Delete(targetDatabase); }
            //File.Copy(resourceITPDatabase, targetDatabase);

            OleDbConnection targetConn = new OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;User ID=Admin; Data Source=" + targetDatabase);

            try
            {
                granConverter.grabGranite(sourceConn, targetConn);
            }

            catch (Exception ex)
            {
                Console.WriteLine("Failed to Convert database: " + sourceDatabase + "\n\nSpecific Error: " + ex);
                System.Windows.Forms.MessageBox.Show("Failed to Convert database: " + sourceDatabase + "\n\nSpecific Error: " + ex);
                //File.Delete(targetDatabase);

            }
            finally
            {
                sourceConn.Close();
                targetConn.Close();
                sourceConn.Dispose();
                targetConn.Dispose();
            }
            System.Windows.Forms.MessageBox.Show("Conversion Completed.");
        }

        private string formatITpipesDBName(string targetDBPath)
        {
            int extensionDotIndex = targetDBPath.LastIndexOf('.');
            string returnFileName = targetDBPath.Substring(0, extensionDotIndex);
            returnFileName += "_ITpipes.mdb";
            return returnFileName;
        }

    }
}
