## Configure AWS

- export AWS_PROFILE="<profile>"
- aws sso login

## Run


```
python main.py --tags lime-energy:stack:env:joao-temp lime-energy:auto-reset:opt:in lime-energy:stack:env:joao-temp-exclude --exclude-tags lime-energy:auto-reset:opt:out
```

