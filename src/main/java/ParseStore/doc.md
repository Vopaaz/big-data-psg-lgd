# Documentation for ParseStore Component

This component can extract information from a `.dem` file and store it into MongoDB.

## Prerequisite

- Java **8**
- Maven
- MongoDB

## Configuration

In the project root, create a file `config.yml` to provide the database configuration information.

```yaml
MongoDB:
  host: <Database host>
  port: <Database port>
  database: <Name of the MongoDB database>
  collection:
    replay-collection: <Name of the collection to store replay file>
```

You can refer to the [example](//config-sample.yml).

## Using

### API

`ParseStore.ParseStoreExecutor` provides the `parseFileStoreMongo` method.

Params:

- `file`: `String`, path to the `.dem` file
- `matchId`: `String`, unique id of a match

### Execute from console

#### Packaging

```bash
$ mvn -P ParseStore package
```

#### Parse and Store

```bash
$ java -jar target/ParseStore.one-jar.jar "path/to/replay.dem"
```

## Replay MongoDB Collection Specification

### Top-level

Each document in the replay collection contains all information of one match replay,
it follows such format:

```text
{
    _id: <BUILT-IN ID>,
    matchid: <Field reserved for marking each match, value can be customized>
    combatlog:[
        {
            <EVENT-LOG1>
        },
        {
            <EVENT-LOG2>
        },
        ...
    ],
    info:{
        <GAME-INFO>
    },
    chat:[
        {
            <CHAT1>
        },
        {
            <CHAT2>
        },
        ...
    ],
    lifestate:[
        {
            <LIFE-EVENT1>
        },
        {
            <LIFE-EVENT2>
        },
        ...
    ]
}
```



### Combatlog

*Explanation of "time": The time is a float representing the total seconds since the game starts.*

Different type of events in the combatlog have different inner structure.

#### Damage

A hero deals damage to another. The fields are:

- time
- type (arbitrarily "damage")
- attacker
- target
- inflictor (seems to be hero ability name)
- damage
- before_hp
- after_hp


#### Heal

A hero heals another. The fields are:

- time
- type (arbitrarily "heal")
- healer
- inflictor
- target
- health
- before_hp
- after_hp

#### Add buff

A hero gets a new buff/debuff. The fields are:

- time
- type (arbitrarily "add_buff")
- target
- inflictor
- attacker


#### Lose buff

The buff/debuff of a hero is removed. The fields are:

- time
- type (arbitrarily "lose_buff")
- target
- inflictor

#### Death

A hero dies. The fields are:

- time
- type (arbitrarily "death")
- target
- killer


#### Ability

A hero uses his/her ability. The fields are:

- time
- type (arbitrarily "ability")
- ability_type ([toggle_on|toggle_off|cast])
- level (of the ability)
- target

#### Item

A hero uses an item. The fields are:

- time
- type (arbitrarily "item")
- user
- item

#### Gold

A hero gets/loses some gold. The fields are:

- time
- type (arbitrarily "gold")
- target
- change (<0 implies lose gold, >0 implies get gold)

#### XP

A hero gains some XP. The fields are:

- time
- type (arbitrarily "XP")
- target
- xp

#### Purchase

A hero buys some item. The fields are:

- time
- type (arbitrarily "purchase")
- target
- item

#### Buyback

A hero buys back. The fields are:

- time
- type (arbitrarily "buyback")
- slot






### Info

Contains game meta information. The fields are:

- game_winner: An integer, either 2 or 3 representing the winning team.
- leagueid
- match_id
- end_time: A very large integer, have different meaning with the "time" in Combatlog
- game_mode: integer. Do not know the meaning
- picks_bans:

```text
picks_bans:[
    {
        team: <An integer, either 2 or 3>
        hero_id: <An integer, do not know what hero it represents>
        is_pick: <boolean, is_pick if true else is_ban>
    },
    {
        team:
        hero_id:
        is_pick:
    },
    ...
]
```

- radiant_team_id
- radiant_team_tag
- dire_team_id
- dire_team_tag
- player_info:

```text
player_info:[
    {
        steamid:
        hero_name:
        game_team: <2 or 3, as explained before>
        is_fake_client:
        player_name:
    },
    {
        steamid:
        hero_name:
        game_team:
        is_fake_client:
        player_name:
    },
    ...
]
```

### Chat

In each chat, there are two fields:

- sender
- message

### Lifestate

Each entry is when an object spawns or dies. The fields are:

- tick: Integer, seemingly related to the game time, but do not know the exact meaning
- type: String, either "spawn" or "die"
- object


