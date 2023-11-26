# League of legends — E-Sports Predictions

> This solution ended up winning out of 1836 participants!

![](https://raw.githubusercontent.com/chakrakan/lol-esports-predictions/main/analysis/doc-assets/winner_2.png)

Predicting league of legends team rankings based on in game data and performance.

This solution was:
- able to predict 14/20 teams in the top 20
- able to accurately predict 

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

### Resources

- [Write Up](https://github.com/chakrakan/lol-esports-predictions/blob/main/write-up.md)
- [Sample Rankings](https://github.com/chakrakan/lol-esports-predictions/blob/main/rankings.md)
