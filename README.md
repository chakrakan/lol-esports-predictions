# League of legends — E-Sports Predictions

Predicting league of legends team performance based on in game data

### Running

After cloning this repository:

1. Create your environment from the provided `environment.yml` file: `conda env create --file environment.yml --name env`
2. Activate your environment: `conda activate /path/to/project/env`
3. Adjust the parameters to the 3 provided functions representing the APIs expected in `app/main.py`

```python
if __name__ == "__main__":
    pprint.pprint(get_tournament_rankings("107458335260330212", "Groups"))
    pprint.pprint(get_global_rankings())
    pprint.pprint(
        get_team_rankings(["98767991853197861", "99566404852189289", "106972778172351142", "98767991877340524"])
    )
```
