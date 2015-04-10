using Isis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CS5412_Project3
{
    class Program
    {
        static Dictionary<string, Dictionary<string, string>> data =
           new Dictionary<string, Dictionary<string, string>>();
        static Semaphore go = new Semaphore(0, 1);
        public const int UPDATE = 0;
        public const int LOOKUP = 1;
        public const char DELIMITER = '&';
        public const int NUM_MEMBERS_NEEDED = 20;

        private static string getParameterNameLookup(string s)
        {
            if (!s.Contains(DELIMITER))
            {
                return s;
            }
            else
            {
                return s.Substring(0, s.IndexOf(DELIMITER));
            }
        }

        static string getParameterName(string s)
        /*
         * PRECONDITION: s is a string formatted as param1=val1&param2=val2 etc.
         * This function will return the name of the first parameter 
         */
        {
            return s.Substring(0, s.IndexOf('='));
        }

        static string getParameterValue(string s)
        /*
        * PRECONDITION: s is a string formatted as param1=val1&param2=val2 etc.
        * This function will return the value of the first parameter 
        */
        {
            if (!s.Contains(DELIMITER))
            {
                return s.Substring(s.IndexOf('=') + 1);
            }
            else
            {
                return s.Substring(s.IndexOf('=') + 1, s.IndexOf(DELIMITER) - s.IndexOf('=') - 1);
            }
        }

        static string removeFirstParameter(string s)
        /*
        * PRECONDITION: s is a string formatted as param1=val1&param2=val2 etc.
        * This function will return the string without the first param and first value
        */
        {
            if (!s.Contains(DELIMITER))
            {
                return "";
            }
            else
            {
                return s.Substring(s.IndexOf(DELIMITER)+1);
            }
        }

        public static void set(Group g, string id, string parameters)
        {
            g.OrderedSend(UPDATE, id, parameters);
        }

        public static List<List<string>> get(Group g, string id, string parameters)
        {
            List<List<string>> results = new List<List<string>>();
            g.OrderedQuery(Group.ALL, LOOKUP, id, parameters, new Isis.EOLMarker(), results);
            return results;
        }

        static void Main(string[] args)
        {
            IsisSystem.Start();
            Group g = new Group("IsisGroup");
            int rank = 0;
            g.ViewHandlers += (ViewHandler)delegate(View v)
            {
                IsisSystem.WriteLine("New View: " + v);
                rank = v.GetMyRank();
                Console.WriteLine("My rank is " + rank);
                if (v.members.Length == NUM_MEMBERS_NEEDED)
                {
                    go.Release(1);
                }
            };
            g.Handlers[UPDATE] += (Action<string, string>)delegate(String id, String param)
            {

                //If id isn't in dictionary add it in with a new dictionary
                if (!data.ContainsKey(id))
                {
                    data.Add(id, new Dictionary<string, string>());
                }

                while (param.Length != 0)
                {
                    string attribute = getParameterName(param);
                    string value = getParameterValue(param);
                    if (value.Equals("null"))
                    {
                        if (data[id].ContainsKey(attribute))
                        {
                            //Only delete if it exists
                            data[id].Remove(attribute);
                        }
                    }
                    else
                    {
                        if (data[id].ContainsKey(attribute))
                        {
                            data[id].Remove(attribute);
                            data[id].Add(attribute, value);
                        }
                        else
                        {
                            data[id].Add(attribute, value);
                        }
                    }
                    param = removeFirstParameter(param);
                }
            };
            g.Handlers[LOOKUP] += (Action<string, string>)delegate(String id, String param)
            {
                if (!data.ContainsKey(id)) { return; }
                List<string> results = new List<string>();

                while (param.Length != 0)
                {
                    string attribute = getParameterNameLookup(param);
                    if (data[id].ContainsKey(attribute))
                    {
                        results.Add(attribute + "=" + data[id][attribute]);
                    }
                    param = removeFirstParameter(param);
                }
                g.Reply(results);
            };
            //ISIS cannot marshall the Dictionary<string,Dictionary<string,string>> type
            //This will be left unimplemented unless needed.
            //g.MakeChkpt += (Isis.ChkptMaker)delegate(View v)
            //{
            //    g.SendChkpt(data);
            //    g.EndOfChkpt();
            //};
            //g.LoadChkpt += (Action<Dictionary<string, Dictionary<string, string>>>)
            //                delegate(Dictionary<string, Dictionary<string, string>> d)
            //{
            //    data = d;
            //};

            g.Join();
            Console.WriteLine("Waiting for enough members. NEED: " + NUM_MEMBERS_NEEDED + " HAVE: " + g.GetView().members.Length);
            go.WaitOne();
            Console.WriteLine("ISIS member " + rank + " proceeding");
            testOne(g);
            testTwo(g);
            testThree(g);
            testFour(g);
            IsisSystem.WaitForever();
        }

        private static void testOne(Group g)
        {
            //Test modifying the same value
            //100 sets and a get 
            int rank = g.GetMyRank();
            string ident = "TESTONEIDENTITY";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < 50; ++i)
            {
                string parameters = "name=Kevin&job=consultant&iterationNum=" + i + "&lastISISSetter=" + rank;
                set(g, ident, parameters);
                set(g, ident, parameters);
                Thread.Sleep(1000);
            }
            //Console.WriteLine("Beginning querry");
            string lookup = "name&job&iterationNum&lastISISSetter";
            List<List<string>> res = get(g, ident, lookup);
            sw.Stop();
            Console.WriteLine("Test one time: " + sw.ElapsedMilliseconds);
            //foreach (List<String> list1 in res)
            //{
            //    foreach (string s in list1)
            //    {
            //        Console.WriteLine(s);
            //    }
            //}
        }

        public static void testTwo(Group g)
        {
            //Test modifying different but sometimes
            //overlapping attributes of the same id from different threads
            //Set 100 times, then get
            int rank = g.GetMyRank();
            string ident = "TESTTWOIDENTITY";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string parameters;
            if (rank % 2 == 0) { parameters = "name=Kevin&job=burger flipper&pay=nothing&lastISISSetter=" + rank; }
            else               { parameters = "name=John&mother=Jane&surname=Doe&lastISISSetter=" + rank; }
            for (int i = 0; i < 50; ++i)
            {
                set(g, ident, parameters);
                set(g, ident, parameters);
                Thread.Sleep(1000);
            }
            //Console.WriteLine("Beginning querry");
            string lookup = "name&job&mother&surname&pay&lastISISSetter";
            List<List<string>> res = get(g, ident, lookup);
            sw.Stop();
            Console.WriteLine("Test two time: " + sw.ElapsedMilliseconds);
            //foreach (List<String> list1 in res)
            //{
            //    foreach (string s in list1)
            //    {
            //        Console.WriteLine(s);
            //    }
            //}
        }

        public static void testThree(Group g)
        {
            //Test modifying disjoint IDs
            //1 set, 100 gets.
            int rank = g.GetMyRank();
            string ident = "TESTTHREE_"+ rank;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string parameters = "state=NY&city=NYC&zip=10021&lastISISSetter=" + rank;
            set(g, ident, parameters);
            //Console.WriteLine("Beginning querry");
            string lookup = "state&city&zip&lastISISSetter";
            List<List<string>> res = new List<List<string>>();
            for (int i = 0; i < 50; ++i)
            {
                res = get(g, ident, lookup);
                res = get(g, ident, lookup);
                Thread.Sleep(1000);
            }
            sw.Stop();
            Console.WriteLine("Test three time: " + sw.ElapsedMilliseconds);
            //foreach (List<String> list1 in res)
            //{
            //    foreach (string s in list1)
            //    {
            //        Console.WriteLine(s);
            //    }
            //}
        }

        public static void testFour(Group g)
        {
            //Test nodifying disjoint ids
            //1 set, 100 gets but everyone gets everyone else's info once
            //This is to see if ISIS caches results when the same querry is made 
            //twice with no changes to the data.
            int rank = g.GetMyRank();
            string ident = "TESTFOUR_" + rank;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string parameters = "state=NY&city=NYC&zip=10021&lastISISSetter=" + rank;
            set(g, ident, parameters);
            //Console.WriteLine("Beginning querry");
            string lookup = "state&city&zip&lastISISSetter";
            List<List<string>> res = new List<List<string>>();
            for (int i = 0; i < 100; ++i)
            {
                ident = "TESTFOUR_" + Math.Min(i, NUM_MEMBERS_NEEDED-1);
                res = get(g, ident, lookup);
                Thread.Sleep(500);
                //foreach (List<String> list1 in res)
                //{
                //    foreach (string s in list1)
                //    {
                //        Console.WriteLine(s);
                //    }
                //}
            }
            sw.Stop();
            Console.WriteLine("Test four time: " + sw.ElapsedMilliseconds);
        }
    }
}
