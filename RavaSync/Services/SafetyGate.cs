using System;
using Dalamud.Game.ClientState.Conditions;
using Dalamud.Plugin.Services;

namespace RavaSync.Services
{
    internal sealed class SafetyGate : IDisposable
    {
        private readonly ICondition _cond;
        private DateTime _lastUnsafe = DateTime.UtcNow;

        public SafetyGate(ICondition cond) { _cond = cond; }

        public bool SafeNow(int calmSeconds, bool zoneOnly)
        {
            bool loading = _cond[ConditionFlag.BetweenAreas] || _cond[ConditionFlag.BetweenAreas51];
            bool blocked = _cond[ConditionFlag.WatchingCutscene] || _cond[ConditionFlag.OccupiedInCutSceneEvent]
                        || _cond[ConditionFlag.BoundByDuty] || _cond[ConditionFlag.InCombat];

            bool unsafeNow = loading || blocked;
            if (unsafeNow) _lastUnsafe = DateTime.UtcNow;

            if (zoneOnly)
                return !loading && (DateTime.UtcNow - _lastUnsafe) > TimeSpan.FromSeconds(2);

            return !loading && !blocked &&
                   (DateTime.UtcNow - _lastUnsafe) > TimeSpan.FromSeconds(Math.Max(1, calmSeconds));
        }

        public void Dispose() { }
    }
}
