# Keep all Signals under SignalsRoot

Goal-driven planning introduces generated Signals alongside user-authored Signals. All SignalActors remain created and supervised by SignalsRoot; GoalActors request their creation and watch associated Signals instead of parenting them. This preserves one management boundary and means stopping a GoalActor does not implicitly stop its associated Signals; retiring Signals when a Goal finishes requires an explicit domain policy.
