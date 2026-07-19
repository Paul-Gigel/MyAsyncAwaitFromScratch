using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

AsyncLocal<int> myValue = new();
List<MyTask> tasks = new();
for (int i = 0; i < 1000; i++)
{
    myValue.Value = i;
    tasks.Add(MyTask.Run(delegate
    {
        Console.WriteLine(myValue.Value);
        //Thread.Sleep(1000);
    }));
}
foreach (var t in tasks)
    t.Wait();

class MyTask
{
    private bool _completed;
    private Exception? _exception;
    private Action? _continuation;
    private ExecutionContext? _context;
    public bool IsCompleted
    {
        get
        {
            lock (this)
            {
                return _completed;
            }
        }
    }

    public void SetResult() => Complete(null);

    public void SetException(Exception? exception) => Complete(exception);

    private void Complete(Exception? exception)
    {
        lock(this)
        {
            if (_completed)
                throw new Exception("invalide State");

            _completed = true;
            _exception = exception;

            if (_continuation is null)
                return;

            if (_context is null)
            {
                _continuation();
                return;
            }

            MyThreadPool.QueueUserWorkItem(delegate
            {
                ExecutionContext.Run(_context, static state =>
                {
                    if (state is not Action action)
                        throw new InvalidOperationException("invalid operation");

                    action.Invoke();
                }, _continuation);
            });
        }
    }

    public void Wait()
    {
        ManualResetEventSlim? mres = null;

        lock (this)
        {
            if (_completed is false)
            {
                mres = new ManualResetEventSlim();
                ContinueWith(mres.Set);
            }
        }

        mres?.Wait();

        if (_exception is not null)
        {
            ExceptionDispatchInfo.Throw(_exception);
        }
    }
    public void ContinueWith(Action action)
    {
        lock (this)
        {
            if (!_completed)
            {
                MyThreadPool.QueueUserWorkItem(action);
                return;
            }
            _continuation = action;
            _context = ExecutionContext.Capture();
        }
    }

    public static MyTask Run(Action action)
    {
        MyTask t = new();

        MyThreadPool.QueueUserWorkItem(() =>
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                t.SetException(e);
                return;
            }

            t.SetResult();
        });

        return t;
    }
}

static class MyThreadPool
{
    private static readonly BlockingCollection<(Action action, ExecutionContext? context)> s_workItems = new();

    public static void QueueUserWorkItem(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var context = ExecutionContext.Capture();
        s_workItems.Add((action, context));
    }

    static MyThreadPool()
    {
        for (int i = 0; i < 1; i++)
        {
            new Thread(() =>
            {
                while (true)
                {
                    var (workItem, context) = s_workItems.Take();

                    if (context != null)
                        ExecutionContext.Run(context, static state =>
                        {
                            if (state is not Action action)
                                throw new InvalidOperationException("invalid operation");

                            action.Invoke();
                        },workItem);
                    else
                        workItem();
                }
            })
            { IsBackground = true }.Start();
        }
    }
}