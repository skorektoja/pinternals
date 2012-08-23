package com.differencer.pi.wizards;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.eclipse.core.resources.IContainer;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.dialogs.ErrorDialog;
import org.eclipse.jface.dialogs.IDialogSettings;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.osgi.util.NLS;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.dialogs.WizardExportResourcesPage;
public class ExportConfigurationWizardPage extends WizardExportResourcesPage implements Listener {
	// widgets
	private Combo destinationNameField;
	private Button destinationBrowseButton;
	protected Button overwriteExistingFilesCheckbox;
	protected Button createDirectoryStructureButton;
	protected Button createSelectionOnlyButton;
	// dialog store id constants
	private static final String STORE_DESTINATION_NAMES_ID = "ExportConfigurationWizardPage.STORE_DESTINATION_NAMES_ID"; //$NON-NLS-1$
	private static final String STORE_OVERWRITE_EXISTING_FILES_ID = "ExportConfigurationWizardPage.STORE_OVERWRITE_EXISTING_FILES_ID"; //$NON-NLS-1$
	private static final String STORE_CREATE_STRUCTURE_ID = "ExportConfigurationWizardPage.STORE_CREATE_STRUCTURE_ID"; //$NON-NLS-1$
	// messages
	private static final String SELECT_DESTINATION_MESSAGE = "Select a directory to export to.";
	private static final String SELECT_DESTINATION_TITLE = "Export to Directory";
	protected ExportConfigurationWizardPage(String name, IStructuredSelection selection) {
		super(name, selection);
	}
	public ExportConfigurationWizardPage(IStructuredSelection selection) {
		this("piConfigurationExportPage", selection); //$NON-NLS-1$
		setTitle("File system");
		setDescription("Export resources to the local file system.");
	}
	protected void addDestinationItem(String value) {
		destinationNameField.add(value);
	}
	public void createControl(Composite parent) {
		super.createControl(parent);
		giveFocusToDestination();
		PlatformUI.getWorkbench().getHelpSystem().setHelp(getControl(), PlatformUI.PLUGIN_ID + "." + "file_system_export_wizard_page");
	}
	protected void createDestinationGroup(Composite parent) {
		Font font = parent.getFont();
		// destination specification group
		Composite destinationSelectionGroup = new Composite(parent, SWT.NONE);
		GridLayout layout = new GridLayout();
		layout.numColumns = 3;
		destinationSelectionGroup.setLayout(layout);
		destinationSelectionGroup.setLayoutData(new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.VERTICAL_ALIGN_FILL));
		destinationSelectionGroup.setFont(font);
		Label destinationLabel = new Label(destinationSelectionGroup, SWT.NONE);
		destinationLabel.setText(getDestinationLabel());
		destinationLabel.setFont(font);
		// destination name entry field
		destinationNameField = new Combo(destinationSelectionGroup, SWT.SINGLE | SWT.BORDER);
		destinationNameField.addListener(SWT.Modify, this);
		destinationNameField.addListener(SWT.Selection, this);
		GridData data = new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.GRAB_HORIZONTAL);
		data.widthHint = SIZING_TEXT_FIELD_WIDTH;
		destinationNameField.setLayoutData(data);
		destinationNameField.setFont(font);
		// destination browse button
		destinationBrowseButton = new Button(destinationSelectionGroup, SWT.PUSH);
		destinationBrowseButton.setText("B&rowse...");
		destinationBrowseButton.addListener(SWT.Selection, this);
		destinationBrowseButton.setFont(font);
		setButtonLayoutData(destinationBrowseButton);
		new Label(parent, SWT.NONE); // vertical spacer
	}
	protected void createOptionsGroupButtons(Group optionsGroup) {
		Font font = optionsGroup.getFont();
		createOverwriteExisting(optionsGroup, font);
		createDirectoryStructureOptions(optionsGroup, font);
	}
	protected void createDirectoryStructureOptions(Composite optionsGroup, Font font) {
		// create directory structure radios
		createDirectoryStructureButton = new Button(optionsGroup, SWT.RADIO | SWT.LEFT);
		createDirectoryStructureButton.setText("&Create directory structure for files");
		createDirectoryStructureButton.setSelection(false);
		createDirectoryStructureButton.setFont(font);
		// create directory structure radios
		createSelectionOnlyButton = new Button(optionsGroup, SWT.RADIO | SWT.LEFT);
		createSelectionOnlyButton.setText("Create on&ly selected directories");
		createSelectionOnlyButton.setSelection(true);
		createSelectionOnlyButton.setFont(font);
	}
	protected void createOverwriteExisting(Group optionsGroup, Font font) {
		// overwrite... checkbox
		overwriteExistingFilesCheckbox = new Button(optionsGroup, SWT.CHECK | SWT.LEFT);
		overwriteExistingFilesCheckbox.setText("&Overwrite existing files without warning");
		overwriteExistingFilesCheckbox.setFont(font);
	}
	protected boolean ensureDirectoryExists(File directory) {
		if (!directory.exists()) {
			if (!queryYesNoQuestion("Target directory does not exist.  Would you like to create it?")) { return false; }
			if (!directory.mkdirs()) {
				displayErrorDialog("Target directory could not be created.");
				giveFocusToDestination();
				return false;
			}
		}
		return true;
	}
	protected boolean ensureTargetIsValid(File targetDirectory) {
		if (targetDirectory.exists() && !targetDirectory.isDirectory()) {
			displayErrorDialog("Target directory already exists as a file.");
			giveFocusToDestination();
			return false;
		}
		return ensureDirectoryExists(targetDirectory);
	}
	protected boolean executeExportOperation(FileSystemExportOperation op) {
		op.setCreateLeadupStructure(createDirectoryStructureButton.getSelection());
		op.setOverwriteFiles(overwriteExistingFilesCheckbox.getSelection());
		try {
			getContainer().run(true, true, op);
		} catch (InterruptedException e) {
			return false;
		} catch (InvocationTargetException e) {
			displayErrorDialog(e.getTargetException());
			return false;
		}
		IStatus status = op.getStatus();
		if (!status.isOK()) {
			ErrorDialog.openError(getContainer().getShell(), "Export Problems", null, // no
																														// special
																														// message
					status);
			return false;
		}
		return true;
	}
	public boolean finish() {
		List resourcesToExport = getWhiteCheckedResources();
		if (!ensureTargetIsValid(new File(getDestinationValue()))) { return false; }
		// Save dirty editors if possible but do not stop if not all are saved
		saveDirtyEditors();
		// about to invoke the operation so save our state
		saveWidgetValues();
		return executeExportOperation(new FileSystemExportOperation(null, resourcesToExport, getDestinationValue(), this));
	}
	protected String getDestinationLabel() {
		return "To director&y:";
	}
	protected String getDestinationValue() {
		return destinationNameField.getText().trim();
	}
	protected void giveFocusToDestination() {
		destinationNameField.setFocus();
	}
	protected void handleDestinationBrowseButtonPressed() {
		DirectoryDialog dialog = new DirectoryDialog(getContainer().getShell(), SWT.SAVE | SWT.SHEET);
		dialog.setMessage(SELECT_DESTINATION_MESSAGE);
		dialog.setText(SELECT_DESTINATION_TITLE);
		dialog.setFilterPath(getDestinationValue());
		String selectedDirectoryName = dialog.open();
		if (selectedDirectoryName != null) {
			setErrorMessage(null);
			setDestinationValue(selectedDirectoryName);
		}
	}
	public void handleEvent(Event e) {
		Widget source = e.widget;
		if (source == destinationBrowseButton) {
			handleDestinationBrowseButtonPressed();
		}
		updatePageCompletion();
	}
	protected void internalSaveWidgetValues() {
		// update directory names history
		IDialogSettings settings = getDialogSettings();
		if (settings != null) {
			String[] directoryNames = settings.getArray(STORE_DESTINATION_NAMES_ID);
			if (directoryNames == null) {
				directoryNames = new String[0];
			}
			directoryNames = addToHistory(directoryNames, getDestinationValue());
			settings.put(STORE_DESTINATION_NAMES_ID, directoryNames);
			// options
			settings.put(STORE_OVERWRITE_EXISTING_FILES_ID, overwriteExistingFilesCheckbox.getSelection());
			settings.put(STORE_CREATE_STRUCTURE_ID, createDirectoryStructureButton.getSelection());
		}
	}
	protected void restoreWidgetValues() {
		IDialogSettings settings = getDialogSettings();
		if (settings != null) {
			String[] directoryNames = settings.getArray(STORE_DESTINATION_NAMES_ID);
			if (directoryNames == null) { return; // ie.- no settings stored
			}
			// destination
			setDestinationValue(directoryNames[0]);
			for (int i = 0; i < directoryNames.length; i++) {
				addDestinationItem(directoryNames[i]);
			}
			// options
			overwriteExistingFilesCheckbox.setSelection(settings.getBoolean(STORE_OVERWRITE_EXISTING_FILES_ID));
			boolean createDirectories = settings.getBoolean(STORE_CREATE_STRUCTURE_ID);
			createDirectoryStructureButton.setSelection(createDirectories);
			createSelectionOnlyButton.setSelection(!createDirectories);
		}
	}
	protected void setDestinationValue(String value) {
		destinationNameField.setText(value);
	}
	protected boolean validateDestinationGroup() {
		String destinationValue = getDestinationValue();
		if (destinationValue.length() == 0) {
			setMessage(destinationEmptyMessage());
			return false;
		}
		String conflictingContainer = getConflictingContainerNameFor(destinationValue);
		if (conflictingContainer == null) {
			// no error message, but warning may exists
			String threatenedContainer = getOverlappingProjectName(destinationValue);
			if (threatenedContainer == null) setMessage(null);
			else setMessage(NLS.bind("The project {0} may be damaged after this operation", threatenedContainer), WARNING);
		} else {
			setErrorMessage(NLS.bind("Destination directory conflicts with location of {0}.", conflictingContainer));
			giveFocusToDestination();
			return false;
		}
		return true;
	}
	protected boolean validateSourceGroup() {
		// there must be some resources selected for Export
		boolean isValid = true;
		List resourcesToExport = getWhiteCheckedResources();
		if (resourcesToExport.size() == 0) {
			setErrorMessage("There are no resources currently selected for export.");
			isValid = false;
		} else {
			setErrorMessage(null);
		}
		return super.validateSourceGroup() && isValid;
	}
	protected String destinationEmptyMessage() {
		return "Please enter a destination directory.";
	}
	protected String getConflictingContainerNameFor(String targetDirectory) {
		IPath rootPath = ResourcesPlugin.getWorkspace().getRoot().getLocation();
		IPath testPath = new Path(targetDirectory);
		// cannot export into workspace root
		if (testPath.equals(rootPath)) return rootPath.lastSegment();
		// Are they the same?
		if (testPath.matchingFirstSegments(rootPath) == rootPath.segmentCount()) {
			String firstSegment = testPath.removeFirstSegments(rootPath.segmentCount()).segment(0);
			if (!Character.isLetterOrDigit(firstSegment.charAt(0))) return firstSegment;
		}
		return null;
	}
	private String getOverlappingProjectName(String targetDirectory) {
		IWorkspaceRoot root = ResourcesPlugin.getWorkspace().getRoot();
		IPath testPath = new Path(targetDirectory);
		IContainer[] containers = root.findContainersForLocation(testPath);
		if (containers.length > 0) { return containers[0].getProject().getName(); }
		return null;
	}
}
