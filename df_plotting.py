import pandas as pd
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

print(df_alt)
print(df_az)

# Force actuators plots
f_timestamp_list = df_f_dist["timestamp"].tolist()

df_f_original = pd.DataFrame({"timestamp": f_timestamp_list, "value": [None] * len(f_timestamp_list)})

num_actuators = 150

fig, axes = plt.subplots(num_actuators + 3)

df_f_act_list = [df_f_original] * num_actuators

for index in range(num_actuators):
    value_list = []

    for dist in df_f_dist["forces"]:
        value = float(dist.strip("[").strip("]").split(", ")[index])
        value_list.append(value)

    df_f_act_list[index]["value"] = value_list

    f_plot = df_f_act_list[index].plot(x="timestamp", y="value", ax=axes[index], title="Actuator {0} plot".format(index))

alt_plot = df_alt.plot(x="timestamp", y="value_float", ax=axes[-3], title="ALT plot")
az_plot = df_az.plot(x="timestamp", y="value_float", ax=axes[-2], title="AZ plot")
img_plot = df_images.plot(x="exposition_start", y="id_img", ax=axes[-1], title="ID plot")

plt.tight_layout()
plt.savefig("plots/alt_plot.pdf")
