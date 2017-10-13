using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Txt2Sqlite
{
    class Program
    {
        #region 脏数据处理
        /// <summary>
        /// 结构化对象
        /// </summary>
        class Data
        {
            public string Username { get; set; }
            public string Password { get; set; }
            public string Email { get; set; }
        }

        static Regex reg = new Regex("[\\s]+");
        static Data LineToData(string lineStr)
        {
            string[] line = reg.Replace(lineStr, " ").Split(' ');// 把多个空格分割成一个，再分拆

            var model = new Data();
            for (int i = 0; i < line.Length; i++)
            {
                if (i == 0) model.Username = line[0];
                else if (Regex.IsMatch(line[i], @"\w+@\w+\.\w+"))
                {
                    model.Email = line[i];
                }
                else
                {
                    model.Password = line[i];
                }
            }
            return model;
        }

        static object sign = new object();// 插入锁
        static Stopwatch watch;// 计算时间
        static DbProviderFactory factory = SQLiteFactory.Instance;// 数据库适配器
        static DbConnection conn = factory.CreateConnection();// 数据库连接
        static int sucessCount = 0;// 提交总条数

        static void BulkData(string table, Data[] list)
        {
            lock (sign)
            {
                DbCommand cmd = conn.CreateCommand();
                // 添加参数  
                cmd.Parameters.Add(cmd.CreateParameter());
                cmd.Parameters.Add(cmd.CreateParameter());
                cmd.Parameters.Add(cmd.CreateParameter());

                DbTransaction trans = conn.BeginTransaction();
                try
                {
                    for (var i = list.Length - 1; i >= 0; i--)
                    {
                        cmd.CommandText = string.Format(
                            "insert into [{0}] ([username],[password],[email]) values (?,?,?)", table);
                        cmd.Parameters[0].Value = list[i].Username;
                        cmd.Parameters[1].Value = list[i].Password;
                        cmd.Parameters[2].Value = list[i].Email;

                        cmd.ExecuteNonQuery();
                    }

                    trans.Commit();
                    sucessCount += list.Length;
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }

                Console.WriteLine($"成功提交：{sucessCount} 条，耗时:{ (watch.ElapsedTicks / (decimal)Stopwatch.Frequency).ToString("0.0000")} 秒。");
            }
        }
        #endregion


        static void Main(string[] args)
        {
            // 开始计时  
            watch = new Stopwatch();
            watch.Start();


            if (args.Length == 0)
            {
                Console.WriteLine("filePath 参数不能为空.");
                Console.ReadKey();
                return;
            }
            else if (args.Length == 1)
            {
                Console.WriteLine("dbPath 参数不能为空.");
                Console.ReadKey();
                return;
            }
            else if (args.Length == 2)
            {
                Console.WriteLine("table 参数不能为空.");
                Console.ReadKey();
                return;
            }

            //获取文件路径
            string filePath = args[0];
            string dbPath = args[1];
            string table = args[2];
            //filePath = @"F:\password\tianya_5.txt";
            //dbPath = @"F:\password\leak4pwd\sqlite3\tianya.db";
            //table = "tianya";

            try
            {
                // 连接数据库  
                conn.ConnectionString = string.Format(
                    @"data source={0};Version=3;datetimeformat=Ticks", dbPath);
                conn.Open();

                var taskList = new List<Task>();

                // Create an instance of StreamReader to read from a file.
                // The using statement also closes the StreamReader.
                using (StreamReader sr = new StreamReader(filePath))
                {
                    String line;
                    // Read and display lines from the file until the end of
                    // the file is reached.

                    int page = 100000;
                    int index = 0;
                    var list = new List<Data>();

                    while ((line = sr.ReadLine()) != null)
                    {
                        if (index++ >= page)
                        {
                            // 读写分离，拷贝一份
                            var tmpList1 = new Data[list.Count];
                            list.CopyTo(tmpList1);

                            taskList.Add(Task.Factory.StartNew(() =>
                            {
                                BulkData(table, tmpList1);
                            }));

                            index = 1;
                            list = new List<Data>();
                        }

                        list.Add(LineToData(line));
                    }

                    var tmpList = new Data[list.Count];
                    list.CopyTo(tmpList);
                    // 最后一页
                    taskList.Add(Task.Factory.StartNew(() =>
                    {
                        BulkData(table, tmpList);
                    }));
                }

                // 所有线程接受后，关闭数据库连接
                Task.Factory.ContinueWhenAll(taskList.ToArray(), tasks =>
                {
                    conn.Close();// 关闭连接
                    watch.Stop();// 停止计时
                });
            }
            catch (Exception e)
            {
                // Let the user know what went wrong.
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }

            Console.WriteLine($"所有数据导入完成。");
            Console.ReadKey();
        }
    }
}
