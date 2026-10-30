# Development notebooks

These notebooks are for manual experiments. Production and CI do not use them.

Add the notebook tools to the existing `piddi` environment from the repository
root, then start Jupyter:

```bash
conda env update -n piddi -f notebooks/environment.yml
conda activate piddi
jupyter lab
```
