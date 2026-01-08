from halo import Halo
import time

with Halo(text="Working…", spinner="dots") as spinner:
    for i in range(10):
        spinner.text = f"Processed {i}"
        if i % 3 == 0:
            spinner.text = f"Checkpoint at {i}"
        time.sleep(0.3)
    spinner.succeed("All done")