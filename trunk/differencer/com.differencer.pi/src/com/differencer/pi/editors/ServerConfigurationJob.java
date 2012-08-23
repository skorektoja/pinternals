package com.differencer.pi.editors;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.swt.widgets.Display;
import com.differencer.pi.Activator;
import com.differencer.pi.Differencer;
import com.differencer.pi.nodes.ConfigurationNode;
public class ServerConfigurationJob extends Job {
	private Server description;
	private boolean withChildren;
	public ServerConfigurationJob(String name, Server d, boolean w) {
		super(name);
		description = d;
		withChildren = w;
	}
	@Override
	protected IStatus run(IProgressMonitor monitor) {
		try {
			if (withChildren) Differencer.collectConfiguration(description);
			else {
				ConfigurationNode root = (ConfigurationNode) Differencer.getStructure(description);
				Object[] children = root.getChildren();
				monitor.beginTask("Collect data from " + description.toString(), children.length);
				Differencer.collectConfigurationForNode(root, monitor);
				for (int i = 0; i < children.length; i++) {
					monitor.subTask("I'm doing something here " + i);
					monitor.worked(20);
					if (monitor.isCanceled()) {
						Activator.log(Status.WARNING, "Configuration collection cancelled");
						return Status.CANCEL_STATUS;
					}
				}
			}
		} finally {
			monitor.done();
		}
		Display.getDefault().syncExec(new Runnable() {
			public void run() {
				Activator.log(Status.INFO, "Configuration collected");
				// CompareUI.openCompareEditor(new
				// CompareInput());
			}
		});
		return Status.OK_STATUS;
	}
}
