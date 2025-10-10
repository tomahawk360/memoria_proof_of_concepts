import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

# Import dataframes
df_corrections = pd.read_csv("dataframes/df_corrections.csv")
df_f_dist = pd.read_csv("dataframes/df_f_dist.csv")
df_images = pd.read_csv("dataframes/df_images.csv")
df_additional_data = pd.read_csv("dataframes/df_additional_data.csv")
df_alt_az = pd.read_csv("dataframes/df_alt_az.csv")

#Plotting the dataframes
# Alt and Az plots
df_alt = df_alt_az[(df_alt_az["type"] == "float") & (df_alt_az["group"] == "ALT")]
df_az = df_alt_az[(df_alt_az["type"] == "float") & (df_alt_az["group"] == "AZ")]

# Force actuators plots
f_timestamp_list = df_f_dist["timestamp"].tolist()

df_f_original = pd.DataFrame({"timestamp": f_timestamp_list, "value": [None] * len(f_timestamp_list)})

num_actuators = 150

n_rows = 25
n_cols = 1

n_partitions = 6

df_f_act_list = [df_f_original] * num_actuators

for pndex in range(n_partitions):

    fig, axes = plt.subplots(nrows=n_rows, figsize=(102.4, 51.2))

    for index in range(n_rows):
        value_list = []

        fndex = index + (n_rows * n_cols * pndex)

        for dist in df_f_dist["forces"]:
            value = float(dist.strip("[").strip("]").split(", ")[fndex])
            value_list.append(value)

        df_f_act_list[fndex]["value"] = value_list

        f_plot = df_f_act_list[fndex].plot(x="timestamp", y="value", ax=axes[index], title="Actuator {0} plot".format(fndex))

    plt.tight_layout()
    plt.savefig("plots/f_dist_plot_{0}.pdf".format(pndex))


#fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(102.4, 51.2))
#
#for index in range(n_rows):
#    for jndex in range(n_cols):
#        value_list = []
#
#        fndex = index + (n_rows * jndex)
#
#        for dist in df_f_dist["forces"]:
#            value = float(dist.strip("[").strip("]").split(", ")[fndex])
#            value_list.append(value)
#
#        df_f_act_list[fndex]["value"] = value_list
#
#        f_plot = df_f_act_list[fndex].plot(x="timestamp", y="value", ax=axes[index][jndex], title="Actuator {0} plot".format(fndex))
#
#plt.savefig("plots/f_dist_plot_1.pdf")
#
#fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(102.4, 51.2))
#
#for index in range(n_rows):
#    for jndex in range(n_cols):
#        value_list = []
#
#        fndex = index + (n_rows * jndex) + (n_rows * n_cols)
#
#        for dist in df_f_dist["forces"]:
#            value = float(dist.strip("[").strip("]").split(", ")[fndex])
#            value_list.append(value)
#
#        df_f_act_list[fndex]["value"] = value_list
#
#        f_plot = df_f_act_list[fndex].plot(x="timestamp", y="value", ax=axes[index][jndex], title="Actuator {0} plot".format(fndex))
#
#plt.savefig("plots/f_dist_plot_2.pdf")
#
#fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(102.4, 51.2))
#
#for index in range(n_rows):
#    for jndex in range(n_cols):
#        value_list = []
#
#        fndex = index + (n_rows * jndex) + (n_rows * n_cols * 2)
#
#        for dist in df_f_dist["forces"]:
#            value = float(dist.strip("[").strip("]").split(", ")[fndex])
#            value_list.append(value)
#
#        df_f_act_list[fndex]["value"] = value_list
#
#        f_plot = df_f_act_list[fndex].plot(x="timestamp", y="value", ax=axes[index][jndex], title="Actuator {0} plot".format(fndex))
#
#plt.savefig("plots/f_dist_plot_3.pdf")

fig, axes = plt.subplots(nrows=3, figsize=(51.2, 25.6))

alt_plot = df_alt.plot(x="timestamp", y="value_float", ax=axes[-3], title="ALT plot")
az_plot = df_az.plot(x="timestamp", y="value_float", ax=axes[-2], title="AZ plot")
img_plot = df_images.plot(x="exposition_start", y="id_img", ax=axes[-1], title="ID plot")

plt.tight_layout()
plt.savefig("plots/alt_az_img_plot.pdf")
