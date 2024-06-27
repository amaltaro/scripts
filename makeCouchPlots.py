import matplotlib.pyplot as plt
import numpy as np


labels = ['50', '250', '500', '1000']
old_means = [2037.0, 2109.0, 2184.0, 1963.0]
new_means = [3228.5, 2873.0, 2827.0, 2709]

x = np.arange(len(labels))  # the label locations
width = 0.3  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, old_means, width, label='cpio1_node')
rects2 = ax.bar(x + width/2, new_means, width, label='ssd_node')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Concurrency (requests)')
ax.set_ylabel('Requests/sec')
ax.set_title('Performance of cpio1 volume vs SSD VMs')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.show()