# 安装：pip install psutil
import psutil
#import time
import matplotlib.pyplot as plt

def monitor_cpu_usage(duration=60):
    """监控CPU使用情况"""
    timestamps = []
    cpu_percentages = []
    
    print("开始监控CPU使用率...")
    for i in range(duration):
        cpu_percent = psutil.cpu_percent(interval=1)
        timestamps.append(i)
        cpu_percentages.append(cpu_percent)
        
        # 显示前5个占用CPU最高的进程
        if i % 10 == 0:
            print(f"\n--- 第{i}秒 CPU使用率: {cpu_percent}% ---")
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent']):
                try:
                    if proc.info['cpu_percent'] > 1.0:  # 只显示占用超过1%的进程
                        processes.append(proc.info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            # 按CPU使用率排序
            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            for proc in processes[:5]:
                print(f"PID: {proc['pid']}, 进程: {proc['name']}, CPU: {proc['cpu_percent']}%")
    
    # 绘制图表
    plt.plot(timestamps, cpu_percentages)
    plt.title('CPU使用率监控')
    plt.xlabel('时间 (秒)')
    plt.ylabel('CPU使用率 (%)')
    plt.show()

# 运行监控
monitor_cpu_usage(120)  # 监控2分钟
