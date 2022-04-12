import matplotlib.pyplot as plt

sizes_non_filtered = [153.2, 292.4, 438.2, 560.2]
num_docs_non_filtered = [55919, 111839, 167759, 223679]

sizes_filtered = [130.5, 271.2, 407.4, 536.5]
num_docs_filtered = [30381, 60762, 91143, 121525]

SIZES = sizes_filtered
NUM_DOCS = num_docs_filtered

# b=4, seeds=12, chars=7, workers_fp=12, workers_jaccard=4
non_filtered_dict_12_4 = {
    "ram_usages": [1600, 2900, 4700, 5800],
    "exec_times": [16.3, 37.6, 67.3, 103.8]
}
# b=4, seeds=12, chars=7, workers_fp=12, workers_jaccard=4
filtered_dict_12_4 = {
    "ram_usages": [1300, 2600, 3600, 4600],
    "exec_times": [10.6, 32.6, 57.4, 98.0]
}

# b=3, seeds=12, chars=7, workers_fp=12, workers_jaccard=4
filtered_dict_12_3 = {
    "ram_usages": [1300, 2000, 2900, 3600],
    "exec_times": [6.1, 13.7, 22.5, 31.4]
}

# b=3, seeds=12, chars=7, workers_fp=12, workers_jaccard=4
filtered_dict_10_2 = {
    "ram_usages": [1300, 1900, 2500, 3000],
    "exec_times": [5.3, 11.3, 17.4, 23.8]
}


def plot_stuff(data_dict, label, color, linestyle='-'):

    ram_usages = data_dict["ram_usages"]
    exec_times = data_dict["exec_times"]

    file_sizes_x = True
    if file_sizes_x:
        x_vals = SIZES
        plt.xlabel("Input size (MB)")
    else:
        x_vals = NUM_DOCS
        plt.xlabel("#documents")

    ram_usages_y = False
    if ram_usages_y:
        y_vals = ram_usages
        plt.ylabel("RAM (MB)")
    else:
        y_vals = exec_times
        plt.ylabel("Exec. Time (s)")

    plt.plot(x_vals, y_vals, label=label, color=color, linestyle=linestyle)
    # plt.plot(x_vals, exec_times, label=label_prefix + "- Exec time (s)", color=color, linestyle=linestyle)


if __name__ == '__main__':
    plot_stuff(filtered_dict_12_4, "#hashes=12, #bands=4", "green")
    plot_stuff(filtered_dict_12_3, "#hashes=12, #bands=3", "blue")
    plot_stuff(filtered_dict_10_2, "#hashes=10, #bands=2", "red")

    plt.legend()
    plt.show()
