using System.Threading.Tasks;

namespace ClickHouseMigrator
{
	public interface IMigrator
	{
		Task RunAsync(params string[] args);
	}
}
