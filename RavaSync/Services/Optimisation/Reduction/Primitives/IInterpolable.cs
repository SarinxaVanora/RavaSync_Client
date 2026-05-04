namespace RavaSync.Services.Optimisation.Reduction
{
    public interface IInterpolable<T>
    {
        T Interpolate(T other, double ratio);
    }
}